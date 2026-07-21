ALTER TABLE "workflow"."workflow_runs" ADD COLUMN "run_version" integer;--> statement-breakpoint
ALTER TABLE "workflow"."workflow_runs" ADD COLUMN "last_batch_id" varchar;
