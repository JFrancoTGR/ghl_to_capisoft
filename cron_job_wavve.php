<?php
// ============================================
// CAPISoft -> GHL Stage Sync (THE WAVVE) Cron Job
// - proyecto_id = 4 (THE WAVVE)
// - since_date filter by CAPISoft created_at
// - state file + lock file
// - match GHL contact by email, fallback phone
// - update opportunity stage + contact customField capisoft_stage
// ============================================

if (php_sapi_name() !== 'cli') {
    http_response_code(403);
    exit;
}

// ====== CONFIG ======
$defaultProyectoId = 4;
$defaultSinceDate  = '2025-12-30'; // YYYY-MM-DD

// CAPISoft
$CAPISOFT_BASE  = "https://api-3.capisoftware.com.mx/eu/capi-b/public/api/v2/ventas/oportunidades";
$CAPISOFT_TOKEN = getenv('CAPISOFT_TOKEN') ?: 'CAPISOFT_TOKEN'; // optional

// GHL (LeadConnector API v2)
$GHL_BASE_URL = "https://services.leadconnectorhq.com";
$GHL_API_VER  = "2021-07-28";

// GHL Token
$GHL_TOKEN = "GHL_TOKEN";

// The WAVVE location + pipeline
$GHL_LOCATION_ID = "GHL_LOCATION_ID";
$GHL_PIPELINE_ID = "GHL_PIPELINE_ID"; // The Wavve - Flujo de venta

// Custom field ID in GHL for capisoft_stage
$GHL_CF_CAPISOFT_STAGE_ID = "K0OxBbmucriu7wdZLVmp";

// CAPISoft etapa_id -> GHL pipelineStageId (The WAVVE)
$CAPI_TO_GHL_STAGE = [
    46  => '3c8a2475-ef4f-409b-9acf-9e9bf97de627', // Prospecto
    260 => 'beaf4b07-f014-43cb-8fff-d6f952ae3146', // Buscando contacto
    47  => '25f85a37-4b6f-4460-a2cc-a70abd62c0b4', // Seguimiento
    262 => '79d89924-db28-430c-824c-6f32cb343b5f', // Citado -> Cita
    263 => '10cd5e99-44dc-4fe6-8fb6-976d8a1420f7', // Visita
                                                   // 47  => '8a298486-168c-4b25-b180-d8e1e525f8a1', // Cotización
];

// Contadores
$GLOBALS['api_calls_contacts_search'] = 0;
$GLOBALS['api_calls_opps_search']     = 0;
$GLOBALS['api_calls_put_opp']         = 0;
$GLOBALS['api_calls_put_contact']     = 0;

// ====== CLI ARGS ======
function get_arg($key)
{
    global $argv;
    if (! is_array($argv)) {
        return null;
    }

    foreach ($argv as $a) {
        if (strpos($a, "--{$key}=") === 0) {
            return substr($a, strlen($key) + 3);
        }
    }
    return null;
}

function log_line($file, $line)
{
    file_put_contents($file, $line . PHP_EOL, FILE_APPEND);
}

function to_ts($dt)
{
    if (! $dt) {
        return null;
    }

    $ts = strtotime($dt);
    return ($ts === false) ? null : $ts;
}

function http_json($method, $url, $headers, $payload = null, $timeout = 25)
{
    $ch = curl_init($url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    curl_setopt($ch, CURLOPT_TIMEOUT, $timeout);
    curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 10);

    if ($payload !== null) {
        $json = json_encode($payload, JSON_UNESCAPED_UNICODE);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $json);
    }

    $body = curl_exec($ch);
    $http = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $err  = curl_error($ch);
    curl_close($ch);
    return [$http, $body, $err];
}

function read_state($path)
{
    if (! file_exists($path)) {
        return ['last_run_at' => null, 'by_clave' => []];
    }

    $json = json_decode(file_get_contents($path), true);
    return is_array($json) ? $json : ['last_run_at' => null, 'by_clave' => []];
}

function write_state($path, $state)
{
    file_put_contents($path, json_encode($state, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT));
}

function capi_fetch_opps($url, $token)
{
    $headers = ["Accept: application/json"];
    if ($token) {
        $headers[] = "Authorization: Bearer {$token}";
    }

    return http_json('GET', $url, $headers, null, 25);
}

function ghl_headers($token, $version)
{
    return [
        "Authorization: Bearer {$token}",
        "Version: {$version}",
        "Accept: application/json",
        "Content-Type: application/json",
    ];
}

// Buscar opp en el pipeline (fallback)
// Usa GET /opportunities/search (Search Opportunity)
function ghl_find_opp_by_contact_pipeline($ghlBase, $headers, $locationId, $pipelineId, $contactId, $logFile = null, $clave = null)
{
    $GLOBALS['api_calls_opps_search']++;

    $attempts = [
        [
            "locationId" => $locationId,
            "page"       => 1,
            "limit"      => 10,
            "filters"    => [
                ["field" => "contactId", "operator" => "eq", "value" => $contactId],
                ["field" => "pipelineId", "operator" => "eq", "value" => $pipelineId],
            ],
        ],
        [
            "locationId" => $locationId,
            "page"       => 1,
            "limit"      => 10,
            "filters"    => [
                ["field" => "contact_id", "operator" => "eq", "value" => $contactId],
                ["field" => "pipeline_id", "operator" => "eq", "value" => $pipelineId],
            ],
        ],
    ];

    foreach ($attempts as $idx => $payload) {
        [$http, $body, $err] = http_json('POST', $ghlBase . "/opportunities/search", $headers, $payload, 25);

        if (! $err && $http < 400) {
            $json = json_decode($body, true);
            if (! is_array($json)) {
                return [null, ['reason' => 'parse_error', 'http' => $http, 'attempt' => $idx]];
            }

            $list = $json['opportunities'] ?? ($json['data'] ?? null);
            if (is_array($list) && count($list) > 0) {
                return [$list[0], ['reason' => 'ok', 'http' => $http, 'attempt' => $idx]];
            }

            return [null, ['reason' => 'empty', 'http' => $http, 'attempt' => $idx]];
        }
    }

    if ($logFile) {
        $snippet = isset($body) ? substr((string) $body, 0, 400) : '';
        log_line($logFile, date('c') . " ERROR GHL_OPP_SEARCH_POST clave={$clave} contact={$contactId} http=" . ($http ?? '') . " err=" . ($err ?? '') . " body={$snippet}");
    }

    return [null, ['reason' => 'http_error', 'http' => $http ?? null, 'err' => $err ?? null]];
}

// Busca contacto por email usando POST /contacts/search
// Regresa: [contact|null, opportunity|null]
function ghl_find_contact_and_opp($ghlBase, $headers, $location_id, $pipeline_id, $email, $logFile = null, $clave = null)
{
    if (! $email) {
        return [null, null, ['reason' => 'no_email']];
    }

    $payload = [
        "locationId" => $location_id,
        "page"       => 1,
        "pageLimit"  => 10,
        "filters"    => [
            ["field" => "email", "operator" => "eq", "value" => $email],
        ],
    ];

    $GLOBALS['api_calls_contacts_search']++;

    [$http, $body, $err] = http_json('POST', $ghlBase . "/contacts/search", $headers, $payload, 25);

    if ($err || $http >= 400) {
        if ($logFile) {
            $snippet = substr((string) $body, 0, 200);
            log_line($logFile, date('c') . " ERROR GHL_CONTACT_SEARCH clave={$clave} email={$email} http={$http} err={$err} body={$snippet}");
        }
        return [null, null, ['reason' => 'http_error', 'http' => $http, 'err' => $err]];
    }

    $json = json_decode($body, true);
    if (! is_array($json)) {
        if ($logFile) {
            $snippet = substr((string) $body, 0, 200);
            log_line($logFile, date('c') . " ERROR GHL_CONTACT_SEARCH_PARSE clave={$clave} email={$email} http={$http} body={$snippet}");
        }
        return [null, null, ['reason' => 'parse_error', 'http' => $http]];
    }

    $contacts = $json['contacts'] ?? null;
    if (! is_array($contacts) || count($contacts) === 0) {
        return [null, null, ['reason' => 'empty', 'http' => $http]];
    }

    $contact  = $contacts[0];
    $opps     = $contact['opportunities'] ?? [];
    $foundOpp = null;

    if (is_array($opps)) {
        foreach ($opps as $opp) {
            if (($opp['pipelineId'] ?? null) === $pipeline_id) {
                $foundOpp = $opp;
                break;
            }
        }
    }

    return [$contact, $foundOpp, ['reason' => 'ok', 'http' => $http]];
}

function get_cf_value($contact, $customFieldId)
{
    $cfs = $contact['customFields'] ?? null;
    if (! is_array($cfs)) {
        return null;
    }

    foreach ($cfs as $cf) {
        if (($cf['id'] ?? null) === $customFieldId) {
            return $cf['value'] ?? null;
        }
    }
    return null;
}

function ghl_update_opportunity_stage($ghlBase, $headers, $opportunityId, $pipelineStageId)
{
    $url     = $ghlBase . "/opportunities/" . urlencode($opportunityId);
    $payload = ["pipelineStageId" => $pipelineStageId];

    $GLOBALS['api_calls_put_opp']++;

    return http_json('PUT', $url, $headers, $payload, 25);
}

function ghl_update_contact_capisoft_stage($ghlBase, $headers, $contact_id, $customFieldId, $value)
{
    $url     = $ghlBase . "/contacts/" . urlencode($contact_id);
    $payload = [
        "customFields" => [
            ["id" => $customFieldId, "value" => $value],
        ],
    ];

    $GLOBALS['api_calls_put_contact']++;

    return http_json('PUT', $url, $headers, $payload, 25);
}

// ====== INPUTS ======
$proyectoId = $defaultProyectoId;
$sinceDate  = $defaultSinceDate;

$p = get_arg('proyecto_id');
$s = get_arg('since_date');
if ($p !== null) {
    $proyectoId = (int) $p;
}

if ($s !== null) {
    $sinceDate = (string) $s;
}

if (! preg_match('/^\d{4}-\d{2}-\d{2}$/', $sinceDate)) {
    echo json_encode(['ok' => false, 'error' => 'since_date inválido, usa YYYY-MM-DD']);
    exit(1);
}

if ($proyectoId !== 4) {
    echo json_encode(['ok' => false, 'error' => 'Este script está fijo para The WAVVE (proyecto_id=4).']);
    exit(1);
}

if (! $GHL_TOKEN || $GHL_TOKEN === 'MI_TOKEN_GHL') {
    echo json_encode(['ok' => false, 'error' => 'Configura $GHL_TOKEN con tu token real.']);
    exit(1);
}

// ====== FILES / FOLDERS ======
$storageDir = __DIR__ . "/storage";
$logsDir    = __DIR__ . "/logs";
if (! is_dir($storageDir)) {
    mkdir($storageDir, 0755, true);
}

if (! is_dir($logsDir)) {
    mkdir($logsDir, 0755, true);
}

$stateFile = $storageDir . "/capisoft_state_project_{$proyectoId}.json";
$logFile   = $logsDir . "/capisoft_sync_wavve.log";
$lockFile  = $storageDir . "/capisoft_lock_project_{$proyectoId}.lock";

// ====== LOCK ======
$lockHandle = fopen($lockFile, 'c');
if (! $lockHandle || ! flock($lockHandle, LOCK_EX | LOCK_NB)) {
    exit(0);
}

// ====== RUN ======
$startedAt  = microtime(true);
$ghlHeaders = ghl_headers($GHL_TOKEN, $GHL_API_VER);

$state = read_state($stateFile);
if (! isset($state['by_clave']) || ! is_array($state['by_clave'])) {
    $state['by_clave'] = [];
}

$beforeCount = count($state['by_clave']);

$CAPISOFT_URL = $CAPISOFT_BASE . "?proyecto_id=" . $proyectoId;

[$http, $body, $err] = capi_fetch_opps($CAPISOFT_URL, $CAPISOFT_TOKEN);
if ($err || $http >= 400) {
    log_line($logFile, date('c') . " ERROR CAPI GET proyecto_id={$proyectoId} http={$http} err={$err}");
    flock($lockHandle, LOCK_UN);
    fclose($lockHandle);
    echo json_encode(['ok' => false, 'http' => $http, 'error' => $err ?: 'CAPISoft error']);
    exit(1);
}

$json = json_decode($body, true);
$data = $json['data'] ?? null;
if (! is_array($data)) {
    log_line($logFile, date('c') . " ERROR CAPI PARSE proyecto_id={$proyectoId} body_unexpected");
    flock($lockHandle, LOCK_UN);
    fclose($lockHandle);
    echo json_encode(['ok' => false, 'error' => 'Respuesta inesperada de CAPISoft']);
    exit(1);
}

// orden por clave
usort($data, function ($a, $b) {
    return (int) ($a['clave'] ?? 0) <=> (int) ($b['clave'] ?? 0);
});

$sinceTs = to_ts($sinceDate . ' 00:00:00');

// filtrar por created_at
$filtered = [];
foreach ($data as $o) {
    $createdAt = $o['created_at'] ?? null;
    $createdTs = to_ts($createdAt);
    if ($createdTs === null) {
        continue;
    }

    if ($createdTs >= $sinceTs) {
        $filtered[] = $o;
    }
}

$changesFound     = 0;
$updatesDone      = 0;
$skippedNoMap     = 0;
$skippedNoContact = 0;
$skippedNoOpp     = 0;
$skippedAligned   = 0;
$errorsGhl        = 0;

$changes = [];

foreach ($filtered as $o) {
    $clave = (string) ($o['clave'] ?? '');
    if ($clave === '') {
        continue;
    }

    $current = [
        'etapa_id'    => $o['etapa_id'] ?? null,
        'etapa'       => $o['etapa'] ?? null,
        'updated_by'  => $o['updated_by'] ?? null,
        'created_at'  => $o['created_at'] ?? null,
        'updated_at'  => $o['updated_at'] ?? ($o['created_at'] ?? null),
        'responsable' => $o['responsable'] ?? null,
        'emails'      => $o['emails'] ?? null,
        'telefonos'   => $o['telefonos'] ?? null,
        'id'          => $o['id'] ?? null,
    ];

    $prev = $state['by_clave'][$clave] ?? null;

    // si no existía en state, lo guardamos pero NO actuamos (evita golpes iniciales)
    if (! $prev) {
        $state['by_clave'][$clave] = $current;
        continue;
    }

    $changed = ($prev['etapa_id'] ?? null) != ($current['etapa_id'] ?? null)
        || ($prev['etapa'] ?? null) != ($current['etapa'] ?? null);

    $capEtapaId    = (int) ($current['etapa_id'] ?? 0);
    $targetStageId = $CAPI_TO_GHL_STAGE[$capEtapaId] ?? null;

    // Si no hay mapeo, no podemos sincronizar ni por cambio ni por reversión
    if (! $targetStageId) {
        if ($changed) {
            $changesFound++;
            $skippedNoMap++;
            log_line($logFile, date('c') . " SKIP NO_MAP clave={$clave} etapa_id={$capEtapaId} etapa=\"{$current['etapa']}\"");
        }
        $state['by_clave'][$clave] = $current;
        continue;
    }

    // Intentaremos reconciliar SIEMPRE que exista el lead en state,
    // para poder revertir cambios manuales en GHL aunque CAPISoft no haya cambiado.
    $email = $current['emails'] ?? null;

    [$contact, $opp, $meta] = ghl_find_contact_and_opp(
        $GHL_BASE_URL, $ghlHeaders, $GHL_LOCATION_ID, $GHL_PIPELINE_ID, $email, $logFile, $clave
    );

    if (! $contact) {
        if ($changed) {
            $changesFound++;
        }

        $skippedNoContact++;
        $r     = $meta['reason'] ?? 'unknown';
        $httpm = $meta['http'] ?? '';
        $errm  = $meta['err'] ?? '';
        log_line($logFile, date('c') . " SKIP NO_CONTACT reason={$r} clave={$clave} email={$email} http={$httpm} err={$errm}");
        $state['by_clave'][$clave] = $current;
        continue;
    }

    $contact_id = $contact['id'] ?? null;
    if (! $contact_id) {
        if ($changed) {
            $changesFound++;
        }

        $skippedNoContact++;
        log_line($logFile, date('c') . " SKIP NO_CONTACT_ID clave={$clave} email={$email}");
        $state['by_clave'][$clave] = $current;
        continue;
    }

    if (! $opp || ! is_array($opp)) {

        // fallback: buscar opportunity por contactId + pipelineId
        [$opp2, $m2] = ghl_find_opp_by_contact_pipeline(
            $GHL_BASE_URL, $ghlHeaders, $GHL_LOCATION_ID, $GHL_PIPELINE_ID, $contact_id, $logFile, $clave
        );

        if (! $opp2 || ! is_array($opp2)) {
            if ($changed) {$changesFound++;}

            $skippedNoOpp++;
            $r2 = $m2['reason'] ?? 'unknown';
            $h2 = $m2['http'] ?? '';
            $e2 = $m2['err'] ?? '';
            log_line($logFile, date('c') . " SKIP NO_OPP reason={$r2} clave={$clave} contact={$contact_id} pipeline={$GHL_PIPELINE_ID} http={$h2} err={$e2}");

            $state['by_clave'][$clave] = $current;
            continue;
        }

        $opp = $opp2;
    }

    $oppId          = $opp['id'] ?? null;
    $currentStageId = $opp['pipelineStageId'] ?? null;

    if (! $oppId) {
        if ($changed) {
            $changesFound++;
        }

        $skippedNoOpp++;
        log_line($logFile, date('c') . " SKIP NO_OPP_ID clave={$clave} contact={$contact_id}");
        $state['by_clave'][$clave] = $current;
        continue;
    }

    // Auditoría: solo agregamos a "changes" cuando CAPISoft cambió (como antes)
    if ($changed) {
        $changesFound++;
        $changes[] = [
            'clave'       => $clave,
            'capi_opp_id' => $current['id'],
            'from'        => ['etapa_id' => $prev['etapa_id'] ?? null, 'etapa' => $prev['etapa'] ?? null],
            'to'          => ['etapa_id' => $current['etapa_id'], 'etapa' => $current['etapa']],
            'email'       => $current['emails'],
            'tel'         => $current['telefonos'],
        ];
    }

    // Siempre intentamos mantener capisoft_stage como auditoría
    $capisoftStageValue = $proyectoId . "|" . $capEtapaId . "|" . ($current['etapa'] ?? '');

    // Reconciliación SOLO POR STAGE
    $needsStageSync = ($currentStageId !== $targetStageId);

    // 1) Si GHL ya está alineado (stage), no tocamos opportunity.
    // Solo aseguramos custom field si hace falta.
    if (! $needsStageSync) {

        $existing = get_cf_value($contact, $GHL_CF_CAPISOFT_STAGE_ID);

        // NO-OP total: ni opp ni custom field requieren cambio
        if ($existing === $capisoftStageValue) {
            $skippedAligned++;
            $state['by_clave'][$clave] = $current;
            continue;
        }

        // Aquí SÍ hay acción: solo update del custom field
        [$ch, $cb, $ce] = ghl_update_contact_capisoft_stage(
            $GHL_BASE_URL, $ghlHeaders, $contact_id, $GHL_CF_CAPISOFT_STAGE_ID, $capisoftStageValue
        );

        if ($ce || $ch >= 400) {
            $errorsGhl++;
            log_line($logFile, date('c') . " ERROR GHL_CONTACT_UPDATE clave={$clave} contact={$contact_id} http={$ch} err={$ce}");
        } else {
            $updatesDone++;
            log_line($logFile, date('c') . " OK GHL_CONTACT_ONLY clave={$clave} contact={$contact_id} capisoft_stage=\"{$capisoftStageValue}\"");
        }

        $state['by_clave'][$clave] = $current;
        continue;
    }

    // 2) Forzamos stage
    [$oh, $ob, $oe] = ghl_update_opportunity_stage($GHL_BASE_URL, $ghlHeaders, $oppId, $targetStageId);

    if ($oe || $oh >= 400) {
        $errorsGhl++;
        log_line($logFile, date('c') . " ERROR GHL_OPP_STAGE_UPDATE clave={$clave} opp={$oppId} http={$oh} err={$oe} body=" . substr((string) $ob, 0, 500));
        $state['by_clave'][$clave] = $current;
        continue;
    }

    log_line($logFile, date('c') . " OK GHL_OPP_STAGE_UPDATE clave={$clave} opp={$oppId} stage={$targetStageId} (reconcile)");

    // 3) Actualizamos el custom field como auditoría
    [$ch, $cb, $ce] = ghl_update_contact_capisoft_stage(
        $GHL_BASE_URL, $ghlHeaders, $contact_id, $GHL_CF_CAPISOFT_STAGE_ID, $capisoftStageValue
    );

    if ($ce || $ch >= 400) {
        $errorsGhl++;
        log_line($logFile, date('c') . " ERROR GHL_CONTACT_UPDATE clave={$clave} contact={$contact_id} http={$ch} err={$ce} body=" . substr((string) $cb, 0, 500));
    } else {
        $updatesDone++;
        log_line($logFile, date('c') . " OK GHL_CONTACT_UPDATE clave={$clave} contact={$contact_id} capisoft_stage=\"{$capisoftStageValue}\"");
    }

    $state['by_clave'][$clave] = $current;
    continue;
}

$state['last_run_at'] = date('c');
write_state($stateFile, $state);

// liberar lock
flock($lockHandle, LOCK_UN);
fclose($lockHandle);

$elapsedMs = (int) ((microtime(true) - $startedAt) * 1000);

echo json_encode([
    'ok'                     => true,
    'proyecto_id'            => $proyectoId,
    'since_date'             => $sinceDate,
    'total_capisoft'         => count($data),
    'observed_created_since' => count($filtered),
    'state_entries_before'   => $beforeCount,
    'state_entries_after'    => count($state['by_clave']),
    'changes_found'          => $changesFound,
    'updates_done'           => $updatesDone,
    'skipped_no_map'         => $skippedNoMap,
    'skipped_no_contact'     => $skippedNoContact,
    'skipped_no_opp'         => $skippedNoOpp,
    'skipped_same_stage'     => $skippedAligned,
    'errors_ghl'             => $errorsGhl,
    'elapsed_ms'             => $elapsedMs,
    'changes'                => $changes,
    'api_calls'              => [
        'contacts_search' => $GLOBALS['api_calls_contacts_search'],
        'opps_search'     => $GLOBALS['api_calls_opps_search'],
        'put_opp'         => $GLOBALS['api_calls_put_opp'],
        'put_contact'     => $GLOBALS['api_calls_put_contact'],
    ],
], JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);

exit(0);
