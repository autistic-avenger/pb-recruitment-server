# How to run?
- Install [air](https://github.com/air-verse/air?tab=readme-ov-file#installation)
- Run `air` command in the terminal
- Server is running on `http://localhost:8080`
- API documentation (Swagger UI): `http://localhost:8080/swagger/index.html`
- Raw JSON Spec: `http://localhost:8080/swagger/doc.json`
- Enable Swagger with `STAGE=dev` or `ENABLE_SWAGGER=true`; otherwise these URLs return 404.

## API Documentation (Swagger)

To regenerate Swagger documentation after modifying route annotations or DTOs:

```bash
make swagger
```

## DB Migrations

- Install [migrate](https://github.com/golang-migrate/migrate)
- Add `DB_ADDR` to `.env`

### Migration Commands

```bash
# Create a new migration
make migration create_users_table

# Run migrations up
make migrate-up

# NOT AVAILABLE ON PROD - Rollback migrations (specify number of steps)
make migrate-down 1

# NOT AVAILABLE ON PROD - Force migration version (use with caution)
make migrate-force 1
```

## Problem authoring

Finish problem edits and deletion before the contest starts. New update and delete
requests return 409 once the contest starts, including after it ends, to preserve
verdicts and leaderboard scores. Finish any in-flight edits before opening the round.
The start time cannot be changed once the contest starts; end-time extensions remain allowed.
Correcting a started contest requires controlled regrading and score reconciliation.

## PostgreSQL tests

Use a disposable PostgreSQL 12+ database. The tests create isolated schemas, apply
all migrations through `000026`, and clean up afterward. They skip when
`TEST_DATABASE_URL` is unset.

```bash
TEST_DATABASE_URL='postgres://user:password@localhost/test_db' go test -race ./internal/stores -count=1
```

## Judge0 deployment

- Apply migrations through `000026` before deploying this build; use PostgreSQL 12+.
- Set `JUDGE0_URL` and `JUDGE0_AUTH_TOKEN`. Keep the Judge0 API inaccessible to contestants.
- Enable callbacks with `JUDGE0_CALLBACK_BASE_URL` and the same `JUDGE0_CALLBACK_SECRET`
  on every backend replica. Verify a signed callback reaches the backend from Judge0.
- With Vivek, load-test queue capacity for 500–600 contestants. The instance checked
  on 11 October has `max_queue_size=100` and `max_memory_limit=512000` KB; our default
  256 MiB is 262144 KB. Raising the queue limit also requires enough workers.
- Recovery fetches results in batches respecting `JUDGE0_BATCH_SIZE` and continues
  across pages within the existing 30-second sweep budget. Abandoned jobs
  are failed only after two valid idle `/workers` observations at least 15 seconds
  apart; candidates must predate the first observation by 30 seconds. Busy queues,
  unavailable statistics, empty queue lists, and queues without workers do not
  trigger failure cleanup. Verify `/workers` reports the execution queue with the
  configured credentials. A permanently unavailable observation endpoint needs
  operator attention; it does not justify discarding potentially valid results.
- Keep callback signatures out of proxy and load-balancer logs as well as application
  logs. The signature is in the callback URL query string.

Verdicts map status 6 to compilation error and 13/14 to `judge_error`. Judge0 has no
dedicated memory-limit verdict. Submission runtime is the sum of testcase runtimes;
memory is the maximum. Scores come from solved problems; freeze problem scores
before starting and use controlled reconciliation for manual database adjustments.
