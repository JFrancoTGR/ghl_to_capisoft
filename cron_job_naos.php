<?php
// ============================================
// CAPISoft -> GHL Stage Sync (NAOS) Cron Job
// - proyecto_id = 3 (NAOS)
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
$defaultProyectoId = 3;
$defaultSinceDate  = '2025-12-30'; // YYYY-MM-DD

// CAPISoft
$CAPISOFT_BASE  = "https://api-3.capisoftware.com.mx/eu/capi-b/public/api/v2/ventas/oportunidades";
$CAPISOFT_TOKEN = getenv('CAPISOFT_TOKEN') ?: 'CAPISOFT_TOKEN'; // optional

// GHL (LeadConnector API v2)
$GHL_BASE_URL = "https://services.leadconnectorhq.com";
$GHL_API_VER  = "2021-07-28";

// GHL Token
$GHL_TOKEN = "GHL_TOKEN";

// NAOS location + pipeline
$GHL_LOCATION_ID = "GHL_LOCATION_ID";
$GHL_PIPELINE_ID = "GHL_PIPELINE_ID"; // NAOS - Flujo de venta

// Custom field ID in GHL for capisoft_stage
$GHL_CF_CAPISOFT_STAGE_ID = "K0OxBbmucriu7wdZLVmp";

// CAPISoft etapa_id -> GHL pipelineStageId (NAOS)
$CAPI_TO_GHL_STAGE = [
    31  => 'a8bb01ea-ca5e-4f0d-ba45-9dd6d0562085', // Prospecto
    256 => 'e23720e0-6c30-4ba1-9289-7f7a4648aa6c', // Buscando contacto
    32 => 'bc58ad63-3c56-4d6b-977a-ae34ae41d772', // Seguimiento
    258 => 'c4f6be4b-4d53-46f3-830b-bf3fe40ed590', // Citado -> Cita
    259 => '5bdaff91-6540-4941-a344-7f9f277795cb', // Visita
    //32  => '543c9c91-08ae-47e3-9b8a-3d2ee6e39ecf', // Cotización
];

// CAPISoft etapa_id terminal -> GHL opportunity status (string)
$CAPI_TO_GHL_STATUS = [
    33 => 'won',       // Apartado -> Ganado
    34 => 'won',       // Vendido  -> Ganado
    35 => 'abandoned', // Cancelado -> Abandonado
    36 => 'lost',      // Cerrado/Perdido -> Perdido
];

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

// Normaliza teléfono a +52XXXXXXXXXX (si ya viene +, respeta)
function normalize_phone($raw)
{
    if (! $raw) {
        return null;
    }

    $s = preg_replace('/[^\d\+]/', '', (string) $raw); // deja dígitos y +
    if ($s === '') {
        return null;
    }

    // si ya viene con +, úsalo
    if (strpos($s, '+') === 0) {
        return $s;
    }

    // si trae 52 al inicio (sin +), conviértelo a +52...
    if (strpos($s, '52') === 0 && strlen($s) >= 12) {
        return '+' . $s;
    }

    // si viene 10 dígitos, asumimos MX +52
    $digits = preg_replace('/\D/', '', $s);
    if (strlen($digits) === 10) {
        return '+52' . $digits;
    }

    // fallback: solo dígitos con +
    return '+' . $digits;
}

// Busca contacto por email o phone usando POST /contacts/search
// Regresa: [contact|null, opportunity|null]
function ghl_find_contact_and_opp($ghlBase, $headers, $locationId, $pipelineId, $email, $phoneNorm)
{

    $tryFilters = [];

    if ($email) {
        $tryFilters[] = [
            'type'    => 'email',
            'payload' => [
                "locationId" => $locationId,
                "page"       => 1,
                "pageLimit"  => 10,
                "filters"    => [
                    ["field" => "email", "operator" => "eq", "value" => $email],
                ],
            ],
        ];
    }

    if ($phoneNorm) {
        $tryFilters[] = [
            'type'    => 'phone',
            'payload' => [
                "locationId" => $locationId,
                "page"       => 1,
                "pageLimit"  => 10,
                "filters"    => [
                    ["field" => "phone", "operator" => "eq", "value" => $phoneNorm],
                ],
            ],
        ];
    }

    foreach ($tryFilters as $t) {
        [$http, $body, $err] = http_json('POST', $ghlBase . "/contacts/search", $headers, $t['payload'], 25);
        if ($err || $http >= 400) {
            // seguimos intentando con el otro criterio si existe
            continue;
        }

        $json     = json_decode($body, true);
        $contacts = $json['contacts'] ?? null;
        if (! is_array($contacts) || count($contacts) === 0) {
            continue;
        }

        $contact = $contacts[0];

        // opportunities pueden venir dentro del contacto (como en tu test)
        $opps     = $contact['opportunities'] ?? [];
        $foundOpp = null;
        if (is_array($opps)) {
            foreach ($opps as $opp) {
                if (($opp['pipelineId'] ?? null) === $pipelineId) {
                    $foundOpp = $opp;
                    break;
                }
            }
        }

        return [$contact, $foundOpp];
    }

    return [null, null];
}

function ghl_update_opportunity_stage($ghlBase, $headers, $opportunityId, $pipelineStageId)
{
    $url     = $ghlBase . "/opportunities/" . urlencode($opportunityId);
    $payload = ["pipelineStageId" => $pipelineStageId];
    return http_json('PUT', $url, $headers, $payload, 25);
}

function ghl_update_opportunity_status($ghlBase, $headers, $opportunityId, $status)
{
    $url     = $ghlBase . "/opportunities/" . urlencode($opportunityId);
    $payload = ["status" => $status]; // 'open' | 'won' | 'lost' | 'abandoned'
    return http_json('PUT', $url, $headers, $payload, 25);
}

function ghl_update_contact_capisoft_stage($ghlBase, $headers, $contactId, $customFieldId, $value)
{
    $url     = $ghlBase . "/contacts/" . urlencode($contactId);
    $payload = [
        "customFields" => [
            ["id" => $customFieldId, "value" => $value],
        ],
    ];
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

if ($proyectoId !== 3) {
    echo json_encode(['ok' => false, 'error' => 'Este script está fijo para NAOS (proyecto_id=3).']);
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
$logFile   = $logsDir . "/capisoft_sync_naos.log";
$lockFile  = $storageDir . "/capisoft_lock_project_{$proyectoId}.lock";

// ====== LOCK ======
$lockHandle = fopen($lockFile, 'c');
if (! $lockHandle || ! flock($lockHandle, LOCK_EX | LOCK_NB)) {
    // Ya hay una ejecución corriendo
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
$skippedAligned = 0;
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
        'created_at'  => $o['created_at'] ?? null, // guardamos created_at como pediste
        'updated_at'  => $o['updated_at'] ?? ($o['created_at'] ?? null),
        'responsable' => $o['responsable'] ?? null,
        'emails'      => $o['emails'] ?? null,
        'telefonos'   => $o['telefonos'] ?? null,
        'id'          => $o['id'] ?? null, // capisoft opp id
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
    $targetStatus  = $CAPI_TO_GHL_STATUS[$capEtapaId] ?? null;

    if ($targetStageId && ! $targetStatus) {
        $targetStatus = 'open';
    }

    // Si NO hay mapeo ni de stage ni de status, entonces sí: no podemos sincronizar
    if (! $targetStageId && ! $targetStatus) {
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
    $email     = $current['emails'] ?? null;
    $phoneNorm = normalize_phone($current['telefonos'] ?? null);

    [$contact, $opp] = ghl_find_contact_and_opp($GHL_BASE_URL, $ghlHeaders, $GHL_LOCATION_ID, $GHL_PIPELINE_ID, $email, $phoneNorm);

    if (! $contact) {
        if ($changed) {
            $changesFound++;
        }

        $skippedNoContact++;
        log_line($logFile, date('c') . " SKIP NO_CONTACT clave={$clave} email={$email} phone={$phoneNorm}");
        $state['by_clave'][$clave] = $current;
        continue;
    }

    $contactId = $contact['id'] ?? null;
    if (! $contactId) {
        if ($changed) {
            $changesFound++;
        }

        $skippedNoContact++;
        log_line($logFile, date('c') . " SKIP NO_CONTACT_ID clave={$clave} email={$email}");
        $state['by_clave'][$clave] = $current;
        continue;
    }

    if (! $opp || ! is_array($opp)) {
        if ($changed) {
            $changesFound++;
        }

        $skippedNoOpp++;
        log_line($logFile, date('c') . " SKIP NO_OPP clave={$clave} contact={$contactId} pipeline={$GHL_PIPELINE_ID}");
        $state['by_clave'][$clave] = $current;
        continue;
    }

    $oppId          = $opp['id'] ?? null;
    $currentStageId = $opp['pipelineStageId'] ?? null;
    $currentStatus  = $opp['status'] ?? null;

    // Vamos a sincronizar stage solo si hay targetStageId (etapas "open")
    $needsStageSync = ($targetStageId && $currentStageId !== $targetStageId);

    // Vamos a sincronizar status solo si hay targetStatus (etapas terminales)
    $needsStatusSync = ($targetStatus && $currentStatus !== $targetStatus);

    if (! $oppId) {
        if ($changed) {
            $changesFound++;
        }

        $skippedNoOpp++;
        log_line($logFile, date('c') . " SKIP NO_OPP_ID clave={$clave} contact={$contactId}");
        $state['by_clave'][$clave] = $current;
        continue;
    }

    // NUEVA REGLA:
    // - si CAPISoft cambió => sync
    // - si CAPISoft NO cambió pero GHL está distinto => revertimos
    //$needsStageSync = ($currentStageId !== $targetStageId);

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

    // 1) Si GHL ya está alineado, no tocamos opportunity; solo aseguramos custom field
    if (! $needsStageSync && ! $needsStatusSync) {
        $skippedAligned++;

        [$ch, $cb, $ce] = ghl_update_contact_capisoft_stage(
            $GHL_BASE_URL, $ghlHeaders, $contactId, $GHL_CF_CAPISOFT_STAGE_ID, $capisoftStageValue
        );

        if ($ce || $ch >= 400) {
            $errorsGhl++;
            log_line($logFile, date('c') . " ERROR GHL_CONTACT_UPDATE clave={$clave} contact={$contactId} http={$ch} err={$ce}");
        } else {
            $updatesDone++;
            log_line($logFile, date('c') . " OK GHL_CONTACT_ONLY clave={$clave} contact={$contactId} capisoft_stage=\"{$capisoftStageValue}\"");
        }

        $state['by_clave'][$clave] = $current;
        continue;
    }

    // 2A) Si hace falta, forzamos stage (solo si hay targetStageId)
    if ($needsStageSync) {
        [$oh, $ob, $oe] = ghl_update_opportunity_stage($GHL_BASE_URL, $ghlHeaders, $oppId, $targetStageId);

        if ($oe || $oh >= 400) {
            $errorsGhl++;
            log_line($logFile, date('c') . " ERROR GHL_OPP_STAGE_UPDATE clave={$clave} opp={$oppId} http={$oh} err={$oe} body=" . substr((string) $ob, 0, 500));
            $state['by_clave'][$clave] = $current;
            continue;
        }

        log_line($logFile, date('c') . " OK GHL_OPP_STAGE_UPDATE clave={$clave} opp={$oppId} stage={$targetStageId} (reconcile)");
    }

    // 2B) Si hace falta, forzamos status (solo si hay targetStatus)
    if ($needsStatusSync) {
        [$sh, $sb, $se] = ghl_update_opportunity_status($GHL_BASE_URL, $ghlHeaders, $oppId, $targetStatus);

        if ($se || $sh >= 400) {
            $errorsGhl++;
            log_line($logFile, date('c') . " ERROR GHL_OPP_STATUS_UPDATE clave={$clave} opp={$oppId} http={$sh} err={$se} body=" . substr((string) $sb, 0, 500));
            $state['by_clave'][$clave] = $current;
            continue;
        }

        log_line($logFile, date('c') . " OK GHL_OPP_STATUS_UPDATE clave={$clave} opp={$oppId} status={$targetStatus} (reconcile)");
    }

    // 3) Actualizamos el custom field como auditoría
    [$ch, $cb, $ce] = ghl_update_contact_capisoft_stage(
        $GHL_BASE_URL, $ghlHeaders, $contactId, $GHL_CF_CAPISOFT_STAGE_ID, $capisoftStageValue
    );

    if ($ce || $ch >= 400) {
        $errorsGhl++;
        log_line($logFile, date('c') . " ERROR GHL_CONTACT_UPDATE clave={$clave} contact={$contactId} http={$ch} err={$ce} body=" . substr((string) $cb, 0, 500));
    } else {
        $updatesDone++;
        log_line($logFile, date('c') . " OK GHL_CONTACT_UPDATE clave={$clave} contact={$contactId} capisoft_stage=\"{$capisoftStageValue}\"");
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
], JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);

exit(0);
