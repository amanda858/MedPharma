# Premium Implementation Backlog

## Goal
Build a trust-first, operator-fast, enterprise-ready platform that is visibly better than competing lead and enrichment systems for healthcare operations.

## Product Pillars
1. Trust and explainability: every field has provenance and confidence.
2. Time to value: first useful report in under 10 minutes.
3. Human speed: review queues and batch actions for operators.
4. Reporting quality: executive-ready packs, not raw exports only.
5. Enterprise readiness: governance, auditability, and reliability.

## North Star Metrics
1. Time to first usable report: less than 10 minutes.
2. Verified contact rate: at least 65 percent on new batches.
3. Manual correction rate: less than 10 percent per import.
4. Batch completion reliability: at least 99 percent successful jobs.
5. Weekly active users by role: admin, staff, client.

## Delivery Plan (90 Days)

### Phase 1 (Days 1-30): Trust and Operations Core
- Confidence scoring and provenance API + UI.
- Job Center with live status and failure diagnostics.
- Executive report pack v1 (PDF + CSV + summary JSON).

### Phase 2 (Days 31-60): Premium Workflow
- Human review cockpit for uncertain records.
- Explainable dedupe and merge preview.
- Role-based onboarding walkthroughs.

### Phase 3 (Days 61-90): Competitive Moat
- Vertical playbooks (lab, pharma, provider group).
- Collaboration approvals with immutable change history.
- Enterprise controls (SSO/SAML prep, SCIM mapping, policy controls).

## EPIC A: Confidence and Provenance

### API Endpoints
1. GET /hub/api/leads/{lead_id}/explain
- Returns confidence score, reasons, source list, stale flags.

2. POST /hub/api/leads/confidence/recompute
- Recomputes confidence for selected lead ids or account scope.

3. GET /hub/api/provenance/{entity_type}/{entity_id}
- Field-level provenance with source, observed_at, verified_at, confidence.

### Data Model Additions
- lead_confidence_snapshots
  - id, lead_id, score, reasons_json, computed_at, model_version
- field_provenance
  - id, entity_type, entity_id, field_name, value, source, observed_at, verified_at, confidence

### UI Screens
1. Lead Detail: Confidence card with reasons and stale warnings.
2. Provenance Drawer: click any field to view source lineage.
3. Batch Recompute panel in account operations.

### Acceptance Criteria
1. Every lead detail view shows score and at least one reason string.
2. At least 90 percent of enriched fields include provenance metadata.
3. Recompute endpoint supports 1000 leads in one request and returns job id.

## EPIC B: Job Center and Reliability

### API Endpoints
1. POST /hub/api/jobs
- Creates async job for import, enrich, verify, report pack.

2. GET /hub/api/jobs/{job_id}
- Returns stage, progress, eta_seconds, retries, latest_error.

3. GET /hub/api/jobs
- Filters by account_id, status, type, created range.

4. POST /hub/api/jobs/{job_id}/retry
- Retries failed stage with idempotency checks.

### Data Model Additions
- jobs
  - id, account_id, job_type, status, progress, eta_seconds, started_at, finished_at, created_by
- job_events
  - id, job_id, stage, message, level, created_at

### UI Screens
1. Job Center list with filters and status chips.
2. Job detail timeline with stage-by-stage logs.
3. Retry action for failed jobs with guard rails.

### Acceptance Criteria
1. All import and report operations run through jobs.
2. Progress updates at least every 5 seconds while active.
3. Failed jobs always include actionable latest_error text.

## EPIC C: Human Review Cockpit

### API Endpoints
1. GET /hub/api/reviews/queue
- Returns uncertain records ordered by risk and impact.

2. PATCH /hub/api/reviews/{review_id}
- Approve, reject, or request verification.

3. POST /hub/api/reviews/bulk
- Bulk triage actions with audit reason.

### Data Model Additions
- review_queue
  - id, account_id, entity_type, entity_id, confidence, risk_level, suggested_action, status
- review_actions
  - id, review_id, action, actor_user_id, reason, created_at

### UI Screens
1. Triage queue with keyboard shortcuts.
2. Side-by-side compare for source conflicts.
3. Bulk action bar with undo window.

### Acceptance Criteria
1. Operators can process at least 100 records in under 15 minutes.
2. Every decision writes immutable review action history.
3. Bulk actions support dry-run preview before commit.

## EPIC D: Executive Reporting Pack

### API Endpoints
1. POST /hub/api/reports/executive-pack
- Generates PDF, CSV, and summary JSON in one job.

2. GET /hub/api/reports/executive-pack/{pack_id}
- Returns artifacts and quality metrics.

3. GET /hub/api/reports/trends
- Time-series metrics for quality and throughput.

### Data Model Additions
- report_packs
  - id, account_id, period_start, period_end, generated_by, generated_at, metrics_json
- report_artifacts
  - id, pack_id, artifact_type, file_path, sha256, created_at

### UI Screens
1. Reports dashboard with trend cards.
2. Executive pack generator with date presets.
3. Download center with artifact integrity indicators.

### Acceptance Criteria
1. Pack generation completes in under 2 minutes for up to 20k records.
2. PDF includes summary, trends, exceptions, and audit appendix.
3. Downloads are role-gated and recorded in audit events.

## EPIC E: Onboarding and Personalization

### API Endpoints
1. GET /hub/api/onboarding/state
- Returns role-specific checklist and completion.

2. POST /hub/api/onboarding/complete-step
- Marks checklist step complete with metadata.

3. GET /hub/api/views
- Returns saved filters, columns, pinned dashboards.

4. POST /hub/api/views
- Saves personalized view presets.

### Data Model Additions
- onboarding_progress
  - id, user_id, role, step_key, completed_at, meta_json
- user_views
  - id, user_id, name, context, config_json, is_default

### UI Screens
1. Guided setup modal by role.
2. Personalized dashboard layout manager.
3. Saved view picker across tables.

### Acceptance Criteria
1. New admin can complete first-run flow in under 10 minutes.
2. Users can save and restore at least 5 custom views.
3. Onboarding completion state persists across sessions.

## EPIC F: Governance and Security

### API Endpoints
1. GET /hub/api/audit/events
- Query by actor, entity, date range, action type.

2. POST /hub/api/audit/export
- Export signed CSV for compliance reviews.

3. GET /hub/api/access/policies
- Returns active role and field policies.

4. POST /hub/api/access/policies/validate
- Dry-run policy impact before apply.

### Data Model Additions
- audit_events
  - id, actor_user_id, action, entity_type, entity_id, before_json, after_json, created_at
- access_policies
  - id, role, scope, field_name, rule, updated_at

### UI Screens
1. Audit explorer with advanced filters.
2. Policy editor with simulation mode.
3. Compliance export center.

### Acceptance Criteria
1. All sensitive writes produce audit events with before and after payloads.
2. Policy simulation returns expected allow/deny matrix before publish.
3. Export includes checksum and export metadata header.

## Immediate Build Order (Sprint-Ready)

### Sprint 1 (2 weeks)
1. Build jobs and job_events tables.
2. Move import and report tasks to async jobs.
3. Build Job Center list and detail UI.
4. Add basic retry flow.

### Sprint 2 (2 weeks)
1. Add lead_confidence_snapshots and scoring service.
2. Add explain endpoint and confidence card in lead detail.
3. Add provenance storage for email, phone, company fields.

### Sprint 3 (2 weeks)
1. Build executive-pack generator service.
2. Add reports trend endpoint and dashboard cards.
3. Add download center and artifact tracking.

### Sprint 4 (2 weeks)
1. Build review queue and action APIs.
2. Implement triage UI with bulk actions.
3. Add immutable review action history.

## Engineering Guardrails
1. Idempotency keys required on all async write triggers.
2. Role checks required at route and service layers.
3. P95 API latency target under 400 ms for read endpoints.
4. Background job retries use exponential backoff with cap.
5. No destructive merge without dry-run preview available.

## Competitive Positioning Notes
1. Lead confidence + provenance is your strongest trust moat.
2. Human review cockpit beats black-box automation competitors.
3. Executive pack output shifts value from data export to decision intelligence.
4. Governance and auditability unlock enterprise buyer confidence.

## Definition of Done for Premium Release
1. All six epics have production endpoints, UI flows, and audit logs.
2. North star metrics are measurable from in-app telemetry.
3. Admin onboarding to first report validated under 10 minutes.
4. Role isolation and cross-account protections verified in live environment.
5. Release checklist signed by product, engineering, and operations owners.
