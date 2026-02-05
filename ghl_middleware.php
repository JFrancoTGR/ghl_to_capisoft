<?php
// ghl_middleware.php

header('Content-Type: application/json; charset=utf-8');

date_default_timezone_set('UTC');

// ========= CONFIG =========
const LOG_FILE          = __DIR__ . '/ghl_middleware.log';
const CAPISOFT_ENDPOINT = 'https://api-3.capisoftware.com.mx/eu/capi-b/public/api/services/social_media/catch';
const CAPISOFT_TOKEN    = 'CAPISOFT_TOKEN'; 

// ===== Project map: GHL capisoft_id_project => legible =====
const PROJECT_MAP = [
    // Proyectos Tijuana
    3  => 'NAOS',
    4  => 'THE WAVVE',

    // Proyectos CDMX
    1  => 'JUÁREZ',
    2  => 'LIVERPOOL',
    5  => 'CHILPANCINGO',
    19 => 'TABACALERA',
];

// Mapeo recomendado: por email del owner (user.email) => responsable_id
$OWNER_EMAIL_TO_RESPONSABLE_ID = [

    // ASESORES TIJUANA
    'valeria.guerrero@brg.mx'           => 145,
    'sandra.guerrero@brg.mx'            => 148,
    'cinthya.pinkus@brg.mx'             => 149,
    'liz.osuna@brg.mx'                  => 150,
    'erick.bada@brg.mx'                 => 151,
    'alejandra.garrido@brg.mx'          => 187,
    'jesus.gallardo@brg.mx'             => 190,
    'karen.parra@brg.mx'                => 212,
    'verenice.maldonado@brg.mx'         => 216,
    'fernanda.gracia@brg.mx'            => 217,
    'cristian.soto@brg.mx'              => 218,
    'marco.morales@brg.mx'              => 222,

    // ASESORES CDMX
    'djalife@estrategiaurbana.com.mx'   => 159,
    'rsantiago@estrategiaurbana.com.mx' => 63,
    'jarceo@estrategiaurbana.com.mx'    => 65,
    'ssantos@estrategiaurbana.com.mx'   => 142,
];

// ========= HELPERS =========
function log_line(array $arr): void
{
    $line = json_encode($arr, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    file_put_contents(LOG_FILE, $line . PHP_EOL, FILE_APPEND);
}

function normalize_platform($value): string
{
    $s = strtolower(trim((string) $value));

    if ($s === 'fb' || $s === 'facebook') {
        return 'facebook';
    }

    if ($s === 'ig' || $s === 'insta' || $s === 'instagram') {
        return 'instagram';
    }

    if ($s === 'meta') {
        return 'facebook';
    }

    if ($s === 'google' || $s === 'googleads' || $s === 'gads') {
        return 'google';
    }

    if ($s === 'manual ghl' || $s === 'manual_ghl' || $s === 'ghl' || $s === 'manual') {
        return "Manual GHL";
    }

    if ($s === 'web' || $s === 'landing') {
        return $s;
    }

    if ($s === '' || $s === 'unknown') {
        return 'unknown';
    }

    return $s;
}

function detect_platform_from_ghl(array $payload, ?string $fallback = null): string
{
    // 1) First-touch real (si existe)
    $src = $payload['contact']['attributionSource']['source'] ?? null;

    // 2) Campo controlado por workflow (lo que necesitamos para Manual Lead)
    if (! $src) {
        $src = $payload['capisoft_platform'] ?? null;
    }
    // recomendado
    if (! $src) {
        $src = $payload['capisoft_manual_platform'] ?? null;
    }
    // si lo creaste así

    // 3) Campo explícito "platform" si lo mandas
    if (! $src) {
        $src = $payload['platform'] ?? null;
    }

    // 4) Alternativa: contact_source
    if (! $src) {
        $src = $payload['contact_source'] ?? null;
    }

    // 5) Fallback final
    if (! $src) {
        $src = $fallback;
    }

    return normalize_platform($src);
}

// function build_fullname($first, $last): ?string
// {
//     $first = trim((string)$first);
//     $last  = trim((string)$last);
//     $full  = trim($first . ' ' . $last);
//     return $full !== '' ? $full : null;
// }

function capisoft_post(array $payload, string $token): array
{
    $ch = curl_init(CAPISOFT_ENDPOINT);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
    curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));

    $headers = ['Content-Type: application/json'];
    if ($token !== '') {
        $headers[] = 'Authorization: Bearer ' . $token;
    }
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);

    $respBody = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlErr  = curl_error($ch);
    curl_close($ch);

    return [$httpCode, $respBody, $curlErr];
}

function get_token(): string
{
    // 1) env var (ideal)
    $t = getenv('CAPISOFT_TOKEN');
    if (is_string($t) && trim($t) !== '') {
        return trim($t);
    }

                                                  // 2) fallback opcional: config.php fuera de public_html (recomendado)
                                                  // Crea /home/USER/config/capisoft.php con: <?php return ['CAPISOFT_TOKEN' => '...'];
                                                  // y ajusta ruta si aplica.
    $fallback = __DIR__ . '/capisoft_config.php'; // si quieres ponerlo junto (NO ideal), úsalo aquí
    if (file_exists($fallback)) {
        $cfg = include $fallback;
        if (is_array($cfg) && ! empty($cfg['CAPISOFT_TOKEN'])) {
            return (string) $cfg['CAPISOFT_TOKEN'];
        }

    }

    // 3) si no hay, vacío (y fallará por 401 si CAPISoft lo requiere)
    return '';
}

function safe_str($v): ?string
{
    $s = trim((string) $v);
    return $s === '' ? null : $s;
}

// ========= MAIN =========
$raw     = file_get_contents('php://input');
$payload = json_decode($raw, true);

if (! is_array($payload)) {
    http_response_code(400);
    echo json_encode(['status' => 'error', 'message' => 'JSON inválido']);
    exit;
}

// 1) Extraer campos de GHL
// $firstName     = $payload['first_name'] ?? null;
// $lastName      = $payload['last_name'] ?? null;
$fullName = $payload['full_name'] ?? null; //build_fullname($firstName, $lastName);

$email = safe_str($payload['email'] ?? null);
$phone = safe_str($payload['phone'] ?? null); // ideal E.164 ya desde GHL

$utmSource   = safe_str($payload['utm_source'] ?? null);
$utmMedium   = safe_str($payload['utm_medium'] ?? null);
$utmCampaign = safe_str($payload['utm_campaign'] ?? null);

$platformFallback = 'unknown';
$platform         = detect_platform_from_ghl($payload, $platformFallback);

$clientBudget  = $payload['client_budget'] ?? null;
$contactMethod = safe_str($payload['contact_method'] ?? null);

$ownerEmail = safe_str($payload['user']['email'] ?? null);

// Proyecto: parse + map
$projectIdRaw = $payload['capisoft_id_project'] ?? null;
$projectId    = is_numeric($projectIdRaw) ? (int) $projectIdRaw : null;

$projectName = ($projectId && isset(PROJECT_MAP[$projectId]))
    ? PROJECT_MAP[$projectId]
    : null;

// Responsable
$responsableId = ($ownerEmail && isset($OWNER_EMAIL_TO_RESPONSABLE_ID[$ownerEmail]))
    ? (int) $OWNER_EMAIL_TO_RESPONSABLE_ID[$ownerEmail]
    : null;

// 2) Inferencias
//$platform   = normalize_platform($utmSource);
$createTime = gmdate('Y-m-d H:i:s');

// 3) Log de entrada (ya con projectId/Name definidos)
log_line([
    'ts'                       => gmdate('c'),
    'type'                     => 'incoming',
    'ip'                       => $_SERVER['REMOTE_ADDR'] ?? null,
    'contact_id'               => $payload['contact_id'] ?? null,
    'owner_email'              => $ownerEmail,
    'project_id'               => $projectId,
    'project_name'             => $projectName,
    'contact_source'           => $payload['contact_source'] ?? null,
    'attrib_source'            => $payload['contact']['attributionSource']['source'] ?? null,
    'platform_final'           => $platform,
    'capisoft_platform'        => $payload['capisoft_platform'] ?? null,
    'capisoft_manual_platform' => $payload['capisoft_manual_platform'] ?? null,
    'platform_payload'         => $payload['platform'] ?? null,
]);

// 4) Validaciones mínimas
$errors = [];

if (! $fullName) {
    $errors[] = 'fullname missing';
}

if (! $projectId) {
    $errors[] = 'proyecto_id missing';
} elseif (! $projectName) {
    $errors[] = 'proyecto_id not supported in PROJECT_MAP';
}

if (! $responsableId) {
    $errors[] = 'responsable_id missing (mapeo por user.email)';
}

if (! $email && ! $phone) {
    $errors[] = 'necesitas email o phone';
}

if ($errors) {
    log_line([
        'ts'          => gmdate('c'),
        'type'        => 'validation_error',
        'errors'      => $errors,
        'owner_email' => $ownerEmail,
        'project_id'  => $projectId,
    ]);

    http_response_code(422);
    echo json_encode([
        'status'  => 'error',
        'message' => 'Validación falló',
        'errors'  => $errors,
    ]);
    exit;
}

// 5) Construir payload para CAPISoft (asumimos keys alineadas como dices)
$capisoftPayload = [
    'fullname'           => $fullName,
    'email'              => $email,
    'phone'              => $phone,
    'project'            => (int) $projectId,
    'responsable_id'     => (int) $responsableId,
    'create_time'        => $createTime,
    'platform'           => $platform,

    // Extras
    'utm_source'         => $utmSource,
    'utm_medium'         => $utmMedium,
    'utm_campaign'       => $utmCampaign,
    'perfil_del_cliente' => $payload['client_type'] ?? null,
    'primer_contacto'    => $contactMethod,
    'monto_de_inversion' => $clientBudget,
];

// 6) Enviar a CAPISoft
[$httpCode, $respBody, $curlErr] = capisoft_post($capisoftPayload, CAPISOFT_TOKEN);

// 7) Log de respuesta CAPISoft
log_line([
    'ts'           => gmdate('c'),
    'type'         => 'capisoft_response',
    'project_id'   => $projectId,
    'project_name' => $projectName,
    'httpCode'     => $httpCode,
    'curlErr'      => $curlErr ?: null,
    'respBody'     => $respBody,
    'sent'         => $capisoftPayload,
]);

// 8) Responder a GHL
if ($curlErr || $httpCode >= 400) {
    http_response_code(502);
    echo json_encode([
        'status'   => 'error',
        'message'  => 'CAPISoft error',
        'capisoft' => [
            'httpCode' => $httpCode,
            'curlErr'  => $curlErr ?: null,
            'body'     => $respBody,
        ],
    ]);
    exit;
}

echo json_encode([
    'status'   => 'ok',
    'message'  => 'Webhook procesado',
    'project'  => [
        'id'   => $projectId,
        'name' => $projectName,
    ],
    'capisoft' => [
        'httpCode' => $httpCode,
        'curlErr'  => $curlErr ?: null,
        'body'     => $respBody,
    ],
]);
