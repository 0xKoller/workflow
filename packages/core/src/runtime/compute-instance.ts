import { ulid } from 'ulid';

/**
 * Identifier for the compute instance (serverless instance / microVM) this
 * module was loaded into.
 *
 * Vercel exposes no native per-instance id under Fluid compute — all
 * `AWS_LAMBDA_*` env vars are blocked — so we synthesize one at module load.
 * It is stable for the life of the instance and shared across every invocation
 * it handles, including the concurrent invocations Fluid packs onto a single
 * instance; cold starts get fresh ids.
 *
 * Value is a prefixed ULID (`cinst_<ulid>`, matching `wrun_`/`step_`/…); the
 * ULID timestamp encodes the instance's birth time, so ids sort by creation.
 * Emitted as the OpenTelemetry `faas.instance` span attribute.
 */
export const COMPUTE_INSTANCE_ID = `cinst_${ulid()}`;
