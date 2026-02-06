<?php
/**
 * Meta CAPI Backfill (last 7 days) - The WAVVE only
 * Sends event_name=Schedule for opportunities in stages: CITA / VISITA
 * Only for contacts where contact.source == "Facebook" OR attributionSource.medium == "facebook"
 *
 * Run:
 *   php meta_backfill_the_wavve_7days.php
 */

if (php_sapi_name() !== 'cli') {
    http_response_code(403);
    exit("CLI only\n");
}

// -------------------- CONFIG (WAVVE) --------------------
$GHL_TOKEN  = getenv('GHL_TOKEN_WAVVE') ?: 'GHL_TOKEN_WAVVE';
$META_TOKEN = getenv('META_TOKEN_WAVVE') ?: 'META_TOKEN_WAVVE';

$LOCATION_ID = 'LOCATION_ID'; // WAVVE locationId
$DATASET_ID  = 'DATASET_ID';     // Pixel/Dataset modern ID (WAVVE)

$PIPELINE_ID     = 'PIPELINE_ID';
$STAGE_ID_CITA   = 'STAGE_ID_CITA';
$STAGE_ID_VISITA = 'STAGE_ID_VISITA';

$DAYS_BACK = 7;

// -------------------- STORAGE --------------------
$REGISTRY_DIR = __DIR__ . '/storage';
if (!is_dir($REGISTRY_DIR)) {
    mkdir($REGISTRY_DIR, 0775, true);
}
$REGISTRY_PATH = "{$REGISTRY_DIR}/meta_backfill_registry_wavve.json";

// -------------------- VALIDATE CONFIG --------------------
foreach ([
    'GHL_TOKEN'       => $GHL_TOKEN,
    'META_TOKEN'      => $META_TOKEN,
    'PIPELINE_ID'     => $PIPELINE_ID,
    'STAGE_ID_CITA'   => $STAGE_ID_CITA,
    'STAGE_ID_VISITA' => $STAGE_ID_VISITA,
] as $k => $v) {
    if (empty($v) || str_contains($v, 'REPLACE_ME')) {
        exit("Missing/placeholder config: {$k}\n");
    }
}

// -------------------- HELPERS --------------------
function http_json(string $method, string $url, array $headers, ?array $body, int $timeout = 30): array
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CUSTOMREQUEST  => $method,
        CURLOPT_TIMEOUT        => $timeout,
        CURLOPT_HTTPHEADER     => $headers,
    ]);

    if ($body !== null) {
        $payload = json_encode($body, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $payload);
    }

    $raw  = curl_exec($ch);
    $err  = curl_error($ch);
    $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    $json = json_decode((string)$raw, true);
    return [
        'ok'   => ($code >= 200 && $code < 300),
        'code' => $code,
        'raw'  => $raw,
        'json' => $json,
        'err'  => $err,
    ];
}

function ghl_headers(string $token): array
{
    return [
        "Accept: application/json",
        "Content-Type: application/json",
        "Authorization: Bearer {$token}",
        "Version: 2021-07-28",
    ];
}

function sha256_norm(?string $s): ?string
{
    if (!$s) return null;
    $v = trim(mb_strtolower($s));
    return $v === '' ? null : hash('sha256', $v);
}

function normalize_phone(?string $p): ?string
{
    if (!$p) return null;
    $v = preg_replace('/\s+/', '', trim($p));
    if ($v === '') return null;
    // keep + and digits only
    $v = preg_replace('/(?!^\+)[^\d]/', '', $v);
    return $v;
}

function is_meta_lead(array $contact): bool
{
    if (!empty($contact['source']) && mb_strtolower($contact['source']) === 'facebook') return true;

    $attr = $contact['attributionSource'] ?? [];
    if (!empty($attr['medium']) && mb_strtolower($attr['medium']) === 'facebook') return true;

    return false;
}

function load_registry(string $path): array
{
    if (!file_exists($path)) return [];
    $raw = file_get_contents($path);
    $j = json_decode($raw, true);
    return is_array($j) ? $j : [];
}

function save_registry(string $path, array $reg): void
{
    file_put_contents(
        $path,
        json_encode($reg, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE)
    );
}

function ghl_search_opps(string $token, string $locationId, array $payload): array
{
    // This tenant requires locationId in body as well
    $payload['locationId'] = $locationId;

    $url = "https://services.leadconnectorhq.com/opportunities/search?locationId={$locationId}";
    return http_json('POST', $url, ghl_headers($token), $payload);
}

function ghl_get_contact(string $token, string $contactId): array
{
    $url = "https://services.leadconnectorhq.com/contacts/{$contactId}";
    return http_json('GET', $url, ghl_headers($token), null);
}

function meta_send(string $token, string $datasetId, array $event): array
{
    $url = "https://graph.facebook.com/v18.0/{$datasetId}/events";
    $headers = [
        "Content-Type: application/json",
        "Authorization: Bearer {$token}",
    ];
    return http_json('POST', $url, $headers, ['data' => [$event]]);
}

// -------------------- RUN --------------------
$registry = load_registry($REGISTRY_PATH);
$sinceTs  = time() - ($DAYS_BACK * 86400);

echo "== WAVVE backfill last {$DAYS_BACK} days ==\n";
echo "Location: {$LOCATION_ID}\nPipeline: {$PIPELINE_ID}\nCITA stage: {$STAGE_ID_CITA}\nVISITA stage: {$STAGE_ID_VISITA}\n";
echo "Meta dataset: {$DATASET_ID}\nRegistry: {$REGISTRY_PATH}\n(only Meta leads: contact.source=Facebook)\n\n";

$targets = [
    ['type' => 'cita',   'stageId' => $STAGE_ID_CITA],
    ['type' => 'visita', 'stageId' => $STAGE_ID_VISITA],
];

$sent = 0;
$skip = 0;
$fail = 0;

foreach ($targets as $t) {
    $type    = $t['type'];
    $stageId = $t['stageId'];

    echo "-- Stage {$type} --\n";
    $page = 1;

    while (true) {
        // Minimal accepted payload (tenant rejects pipelineId/stageId filters)
        $payload = [
            'limit' => 100,
            'page'  => $page,
        ];

        $res = ghl_search_opps($GHL_TOKEN, $LOCATION_ID, $payload);
        if (!$res['ok']) {
            echo "ERROR opp search HTTP {$res['code']}: {$res['raw']}\n";
            $fail++;
            break;
        }

        $items = $res['json']['opportunities'] ?? $res['json']['data'] ?? [];
        if (!is_array($items) || count($items) === 0) break;

        foreach ($items as $opp) {
            // Required IDs
            $oppId     = $opp['id'] ?? null;
            $contactId = $opp['contactId'] ?? ($opp['contact']['id'] ?? null);
            if (!$oppId || !$contactId) continue;

            // Client-side filtering
            $oppPipelineId = $opp['pipelineId'] ?? null;
            $oppStageId    = $opp['pipelineStageId'] ?? null;

            if ($oppPipelineId !== $PIPELINE_ID) continue;
            if ($oppStageId !== $stageId) continue;

            // Window filter (prefer lastStageChangeAt)
            $stageChangedAt   = strtotime($opp['lastStageChangeAt'] ?? '') ?: null;
            $updatedAtRaw     = strtotime($opp['updatedAt'] ?? '') ?: null;
            $eventTsCandidate = $stageChangedAt ?: $updatedAtRaw;

            if ($eventTsCandidate && $eventTsCandidate < $sinceTs) continue;

            // Idempotent event_id
            $eventId = "meta:4:{$oppId}:{$type}";
            if (isset($registry[$eventId])) { $skip++; continue; }

            // Load contact (to confirm Meta source + get email/phone)
            $c = ghl_get_contact($GHL_TOKEN, $contactId);
            if (!$c['ok']) {
                $registry[$eventId] = [
                    'status'    => 'fail_contact',
                    'ts'        => time(),
                    'oppId'     => $oppId,
                    'contactId' => $contactId,
                    'error'     => "HTTP {$c['code']}",
                ];
                $fail++;
                continue;
            }

            $contact = $c['json']['contact'] ?? [];
            if (!is_meta_lead($contact)) {
                $registry[$eventId] = [
                    'status'    => 'skip_not_meta',
                    'ts'        => time(),
                    'oppId'     => $oppId,
                    'contactId' => $contactId,
                ];
                $skip++;
                continue;
            }

            // Prepare user_data
            $emailHash = sha256_norm($contact['email'] ?? null);
            $phoneNorm = normalize_phone($contact['phone'] ?? null);
            $phoneHash = $phoneNorm ? hash('sha256', $phoneNorm) : null;

            if (!$emailHash && !$phoneHash) {
                $registry[$eventId] = [
                    'status'    => 'skip_no_user_data',
                    'ts'        => time(),
                    'oppId'     => $oppId,
                    'contactId' => $contactId,
                ];
                $skip++;
                continue;
            }

            // event_time must be within last 7 days
            $eventTime = $eventTsCandidate ?: time();
            if ($eventTime < $sinceTs) $eventTime = $sinceTs + 60;

            $event = [
                'event_name'    => 'Schedule',
                'event_time'    => $eventTime,
                'action_source' => 'system_generated',
                'event_id'      => $eventId,
                'user_data'     => array_filter([
                    'em'          => $emailHash ? [$emailHash] : null,
                    'ph'          => $phoneHash ? [$phoneHash] : null,
                    // Meta will hash this internally; keep it stable
                    'external_id' => [$contactId],
                ]),
                'custom_data'   => [
                    'appointment_type'    => $type,
                    'lead_event'          => $type === 'cita' ? 'appointment_scheduled' : 'visit_scheduled',
                    'project_id'          => 4,
                    'project_name'        => 'The WAVVE',
                    'ghl_location_id'     => $LOCATION_ID,
                    'ghl_contact_id'      => $contactId,
                    'ghl_opportunity_id'  => $oppId,
                    'ghl_pipeline_id'     => $PIPELINE_ID,
                    'ghl_pipeline_stage_id'=> $stageId,
                ],
            ];

            $m = meta_send($META_TOKEN, $DATASET_ID, $event);
            if ($m['ok']) {
                $registry[$eventId] = [
                    'status'    => 'sent',
                    'ts'        => time(),
                    'oppId'     => $oppId,
                    'contactId' => $contactId,
                    'meta'      => $m['json'],
                ];
                $sent++;
            } else {
                $registry[$eventId] = [
                    'status'    => 'fail_meta',
                    'ts'        => time(),
                    'oppId'     => $oppId,
                    'contactId' => $contactId,
                    'error'     => $m['raw'],
                ];
                $fail++;
            }
        }

        save_registry($REGISTRY_PATH, $registry);

        $page++;
        if ($page > 50) break; // safety cap
    }
}

save_registry($REGISTRY_PATH, $registry);

echo "\n== DONE WAVVE ==\nSent: {$sent}\nSkipped: {$skip}\nFailed: {$fail}\nRegistry: {$REGISTRY_PATH}\n";
