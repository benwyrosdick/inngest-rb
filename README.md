# Inngest Ruby SDK

A Ruby SDK for [Inngest](https://www.inngest.com): durable functions with retriable steps, sleeps, waits and
flow control. Rails comes first, but the SDK works in any Ruby 3.2+ app.

Functions can run in two ways, and both use the same function code:

- **Connect workers** (recommended): a long-running process with an outbound WebSocket to Inngest, similar to
  a Sidekiq process. Your app doesn't expose an endpoint, there are no request timeouts, and functions sync
  automatically when the worker connects.
- **HTTP serve mode:** a Rack endpoint that Inngest calls. Use this when you can't run long-lived processes,
  e.g. on serverless platforms. See [Serving over HTTP](#serving-over-http).

> **Status:** early. Both modes work end to end against the Inngest dev server and a self-hosted Inngest
> server with signing keys. They haven't yet been run against Inngest Cloud. See [Roadmap](#roadmap) for
> what's still missing.

## Quick start (Rails)

```ruby
# Gemfile
gem "inngest"
```

```
bin/rails generate inngest:install
bin/rails generate inngest:function SendWelcomeEmail --event user/signed_up
```

The install generator does the following:
- creates `config/initializers/inngest.rb`, which uses the local dev server in development
- creates `app/inngest/`
- adds a Connect worker to `Procfile` and `Procfile.dev`
- adds the dev server to `Procfile.dev`

Run `bin/rails generate inngest:install --mode=serve` to mount an HTTP endpoint instead of running a worker.

The function generator creates `app/inngest/send_welcome_email.rb` and a test for it, written for RSpec or
Minitest depending on your app (see [Testing](#testing)). Pass `--cron "0 9 * * *"` for a scheduled function,
or `--skip-test` to skip the test.

Write the function:

```ruby
# app/inngest/send_welcome_email.rb
class SendWelcomeEmail < Inngest::Function
  trigger event: "user/signed_up"
  concurrency limit: 10, key: "event.data.account_id"
  retries 3

  def call
    user = step.run("load-user") { User.find(event.data["user_id"]).slice("id", "email") }
    step.sleep("wait-a-day", 1.day)
    step.run("send-email") { WelcomeMailer.welcome(user["email"]).deliver_now }
  end
end
```

Send events from anywhere in your app:

```ruby
Inngest.send_event(name: "user/signed_up", data: { user_id: user.id, account_id: user.account_id })
```

Run `bin/dev` to start the app, the worker and the dev server. The dev server UI at http://localhost:8288
shows every run and step. In production, set `INNGEST_SIGNING_KEY` and `INNGEST_EVENT_KEY`, and run the worker
next to your web process:

```
# Procfile
web: bundle exec puma -C config/puma.rb
inngest: bundle exec inngest-connect
```

## How it works

Each step's result is stored by Inngest. Every time Inngest calls a function, the function runs again from the
top. Steps that already ran return their stored result immediately, and the first new step runs, gets reported
to Inngest, and ends the call. Two rules follow from this:

- **Code outside steps must be deterministic.** Put side effects, time, randomness and database reads inside
  `step.run`.
- **Step results are JSON.** `step.run` returns what it will return on replay: string keys, no symbols, no
  ActiveRecord objects. This holds even the first time the step runs, so bugs show up right away.

## Functions

Subclass `Inngest::Function`, add at least one trigger, and implement `#call`. Inside `#call` you can use
`event`, `events` (for batches), `step`, `run_id`, `attempt` and `logger`. You can also declare them as keyword
arguments: `def call(event:, step:)`.

```ruby
class ProcessOrder < Inngest::Function
  id "process-order"                  # default: dasherized class name ("process-order")
  display_name "Process order"        # shown in the dashboard

  trigger event: "order/created", if: "event.data.total > 100"
  trigger cron: "TZ=America/New_York 0 9 * * 1-5"

  retries 5
  concurrency limit: 10, key: "event.data.account_id"
  throttle limit: 100, period: 1.minute
  rate_limit limit: 1, period: 1.hour, key: "event.data.user_id"
  debounce period: 30.seconds, key: "event.data.order_id"
  batch_events max_size: 100, timeout: 5.seconds
  cancel_on event: "order/cancelled", if: "event.data.order_id == async.data.order_id"
  priority run: "event.data.priority"
  idempotency "event.data.order_id"
  timeouts start: 10.minutes, finish: 1.hour
  singleton mode: :skip, key: "event.data.user_id"

  def call
    # ...
  end

  # Optional. Runs once after the function fails for good.
  def on_failure
    logger.error("order failed: #{error["message"]}")
  end
end
```

Durations accept seconds, `ActiveSupport::Duration` (`1.day`), or Inngest time strings (`"2h30m"`).

## Steps

```ruby
# Retriable, memoized code.
data = step.run("fetch") { ApiClient.fetch(id) }

# Durable sleeps; the worker isn't busy while a run sleeps.
step.sleep("cool-off", 1.hour)
step.sleep_until("tomorrow-morning", Date.tomorrow.to_time + 9.hours)

# Pause until a matching event arrives (or nil on timeout).
approval = step.wait_for_event("approval", event: "invoice/approved", timeout: 3.days,
                                           if: "async.data.invoice_id == event.data.invoice_id")

# Call another function and wait for its result.
total = step.invoke("price", function: CalculatePrice, data: { "order_id" => id })

# Send events durably.
step.send_event("notify", name: "order/processed", data: { "order_id" => id })
```

Step IDs must be stable. Reusing an ID within a run, for example in a loop, is fine.

### Parallel steps

`step.parallel` takes callables and returns their results in order. Inngest runs the steps in them
concurrently, as separate calls to your workers.

```ruby
invoice, shipment = step.parallel(
  -> { step.run("create-invoice") { Invoice.create_for(order_id).id } },
  -> { step.run("book-shipment") { Shipping.book(order_id) } }
)

# Fan out over a collection; give each step its own ID.
results = step.parallel(line_items.map { |item| -> { step.run("reserve-#{item["sku"]}") { reserve(item) } } })
```

A callable can contain several steps of any type (`run`, `sleep`, `wait_for_event`, `invoke`, ...), which run
in order within it. By default the function waits for each round of parallel steps to finish before moving on.
Pass `mode: :race` to let each callable move to its next step as soon as its current one finishes. That helps
callables with several steps progress independently, at the cost of more calls to your workers.

If a step fails for good, `step.parallel` raises its `Inngest::StepError`.

### Errors

- If a step raises, Inngest retries it with backoff. Once its retries are exhausted, the step raises
  `Inngest::StepError` in your function. You can rescue it; if you don't, the function fails.
- `raise Inngest::NonRetriableError, "..."` fails right away, without retries.
- `raise Inngest::RetryAfterError.new("rate limited", retry_after: 30)` retries no earlier than 30 seconds from
  now. You can also pass a `Time`.

## Testing

`Inngest::Testing.run` runs a function locally, the way Inngest would, without a server. Steps run for real
unless you stub them by step ID, and sleeps finish immediately.

```ruby
# spec/rails_helper.rb
require "inngest/rspec"

RSpec.describe SendWelcomeEmail do
  it "emails the user" do
    result = run_inngest_function(described_class,
      event: { name: "user/signed_up", data: { user_id: 1 } },
      steps: { "load-user" => { "email" => "ada@example.com" } })   # stub: the block doesn't run

    expect(result).to be_completed
    expect(result.step("send-email")).to eq(...)
    expect(result.events).to be_empty                                # events sent with step.send_event
  end
end
```

With Minitest, `include Inngest::Testing::Helpers` in your test case and use the same `run_inngest_function`.

How the helper treats each kind of step:
- **Stubs:** stub by step ID, or `"id:1"` for the second use of a repeated ID. A stub can be a value, an
  exception (the step fails), or a Proc (called for the value).
- **`step.wait_for_event`:** needs a stub. Pass an event hash, or `nil` for a timeout.
- **`step.invoke`:** runs the invoked function locally unless stubbed.
- **Retries:** failing steps and functions are retried up to the function's `retries`. Pass `max_attempts: 1`
  to turn retries off.

The result has:
- `completed?` / `failed?`
- `output` (as JSON)
- `error` (the original exception, or an `Inngest::StepError` for a failed step)
- `steps`, `step(id)` and `ran_step?(id)`
- `events`

`failure: true, error: SomeError.new("...")` runs a function's `#on_failure` handler.

## Middleware

Middleware runs code around every function call and event send, e.g. for logging, error reporting, tracing or
setting up context:

```ruby
class TenantMiddleware < Inngest::Middleware
  def transform_input(input)
    input.context[:tenant] = Tenant.find(input.event.data["tenant_id"])
  end

  def transform_output(output)
    ErrorTracker.report(output.error) if output.error
  end
end

Inngest.configure(app_id: "shop", middleware: [TenantMiddleware])   # every function

class SyncOrders < Inngest::Function
  middleware AuditMiddleware                                          # this function only
  trigger event: "orders/sync"

  def call(tenant:)                                                   # values from input.context
    # ...
  end
end
```

A new instance is created for each call, with `client`, `function` and `request` available. The `request` is a
`Rack::Request` in serve mode, or the gateway's request message with Connect. Client middleware runs before
function middleware, in the order added.

Each call runs these hooks once, in order:

| Hook | When | Can change |
| --- | --- | --- |
| `transform_input(input)` | Before the function runs | `input.event`, `input.events`, stored step results (`input.steps`), and `input.context`, which the function sees as `context[...]` or keyword arguments |
| `after_memoization` | Stored steps have been replayed | |
| `before_execution` | New code is about to run | |
| `after_execution` | The new code stopped | |
| `transform_output(output)` | After a function or step returns or raises | `output.data`, `output.error` |
| `before_response` | Before the result goes to Inngest | |

In `transform_output`:
- `output.finished?` is true for the function's own result.
- `output.step` is set when a step ran.
- When neither applies, the call only scheduled new steps.

`before_send_events(events)` can change events before they're sent, both from `Inngest.send_event` and
`step.send_event`. `after_send_events(result)` gets the IDs and any error.

## Running workers

```
bundle exec inngest-connect [options]
  -r, --require PATH          File to load (default: config/environment.rb if present)
  -c, --concurrency N         Functions to run at once (default: the database pool size, or 10)
  -i, --instance-id ID        Stable worker ID (default: hostname)
  -t, --shutdown-timeout S    Seconds to wait for running steps on shutdown (default: 25)
      --health-port PORT      Serve /ready and /live for Kubernetes probes on PORT
      --health-host HOST      Address for health checks (default: 0.0.0.0)
```

In Rails, the CLI eager-loads the app to find every `Inngest::Function` subclass. Each execution runs inside
the Rails reloader, like a Sidekiq job. With Active Record, concurrency defaults to the database pool size,
since each running function may hold a connection.

On `SIGTERM` or `SIGINT`, the worker stops taking new work and waits for running steps to finish. It delivers
their results and then exits. Inngest retries any step that's still running at the timeout. If the connection
drops, the worker reconnects with backoff. Results that couldn't be sent over the socket are delivered over
HTTP instead.

You can also run a worker from Ruby:

```ruby
client = Inngest::Client.new(app_id: "my-app")
Inngest::Connect::Worker.new(client: client, functions: [SendWelcomeEmail]).run
```

### Kubernetes

With `--health-port`, the worker serves two probe endpoints, each with a small JSON status body:

| Endpoint | 200 when | 503 when |
| --- | --- | --- |
| `/ready` | Connected to Inngest and taking work | Connecting, reconnecting or shutting down |
| `/live` | The worker is running normally | Its background work has stalled for over 60 seconds |

`/readyz`, `/livez` and `/healthz` also work.

```yaml
containers:
  - name: inngest-worker
    command: ["bundle", "exec", "inngest-connect", "--health-port", "9394"]
    env:
      - name: INNGEST_CONNECT_INSTANCE_ID   # a stable name per pod
        valueFrom: { fieldRef: { fieldPath: metadata.name } }
    readinessProbe:
      httpGet: { path: /ready, port: 9394 }
      periodSeconds: 5
    livenessProbe:
      httpGet: { path: /live, port: 9394 }
      periodSeconds: 15
      failureThreshold: 4
terminationGracePeriodSeconds: 30   # longer than --shutdown-timeout (25s)
```

A worker that loses its connection keeps passing liveness while it reconnects on its own, so Kubernetes won't
restart it. Readiness marks the pod as not ready in the meantime, which keeps rolling deploys from moving on
until new workers have connected. On `SIGTERM`, readiness turns 503 right away while running steps finish.

Outside Kubernetes, `Worker#ready?`, `Worker#alive?` and `Worker#health` give the same information.

## Serving over HTTP

Mount `Inngest::Serve`, a Rack app, wherever Inngest can reach it:

```ruby
# config/routes.rb
mount Inngest::Serve.new => "/api/inngest"
```

```ruby
# config.ru for Sinatra, Roda, Hanami, etc.
map("/api/inngest") { run Inngest::Serve.new(functions: [SendWelcomeEmail]) }
```

By default `Inngest::Serve` serves every `Inngest::Function` subclass. In Rails development, where code loads
lazily, it loads `app/inngest/` first. Pass `functions:` to choose which functions to serve.

Sync your functions after each deploy by sending a `PUT` to the endpoint, or by adding the URL in the Inngest
dashboard:

```
curl -X PUT https://example.com/api/inngest
```

For local development, run the dev server with `npx inngest-cli@latest dev -u http://localhost:3000/api/inngest`.

The endpoint handles everything the [SDK spec](https://github.com/inngest/inngest/blob/main/docs/SDK_SPEC.md)
requires:
- **Requests:** verifies Inngest's request signatures (with the fallback key during rotation) and signs its
  responses.
- **Syncing:** registers functions, supports in-band sync, and answers trust probes.
- **Health checks:** a `GET` returns a health check that includes private details only when the request is
  signed.

If Inngest reaches your app through a proxy or a different hostname, set `INNGEST_SERVE_ORIGIN` (e.g.
`https://example.com`) and `INNGEST_SERVE_PATH`. You can also pass `serve_origin:` and `serve_path:`.

Each step is a separate request, so a step holds a web server thread for as long as it runs. Keep steps short,
or use a Connect worker.

## Configuration

| Variable | Purpose |
| --- | --- |
| `INNGEST_APP_ID` | App ID. In Rails, defaults to the application name, e.g. `my-shop`. |
| `INNGEST_SIGNING_KEY` | Required for Inngest Cloud. |
| `INNGEST_SIGNING_KEY_FALLBACK` | Used while rotating signing keys. |
| `INNGEST_EVENT_KEY` | Required to send events to Inngest Cloud. |
| `INNGEST_ENV` | Branch environment name. |
| `INNGEST_DEV` | `1` to use the dev server, or the dev server's URL. |
| `INNGEST_APP_VERSION` | Changes with each deploy (e.g. a git SHA), so Inngest can handle rolling deploys. |
| `INNGEST_BASE_URL`, `INNGEST_API_BASE_URL`, `INNGEST_EVENT_API_BASE_URL` | Override API origins (e.g. for self-hosting). |
| `INNGEST_CONNECT_MAX_WORKER_CONCURRENCY` | Default for `--concurrency`. |
| `INNGEST_CONNECT_INSTANCE_ID` | Default for `--instance-id`. |
| `INNGEST_CONNECT_HEALTH_PORT`, `INNGEST_CONNECT_HEALTH_HOST` | Defaults for `--health-port` and `--health-host`. |
| `INNGEST_SERVE_ORIGIN`, `INNGEST_SERVE_PATH` | Public URL of the serve endpoint, if it differs from what the app sees. |
| `INNGEST_LOG_LEVEL` | Log level for the default logger. |

Or configure in code, for example in an initializer:

```ruby
Inngest.configure(app_id: "my-app", logger: Rails.logger)
```

## Development

```
bundle install
bundle exec rake            # unit specs
bundle exec rubocop

# End-to-end specs (Connect and HTTP serve) against a fresh dev server
npx inngest-cli@latest dev --no-discovery --no-poll
bundle exec rake spec:integration

# The same specs with signed requests and authentication, against self-hosted Inngest
inngest start --signing-key <hex> --event-key <key>
INNGEST_INTEGRATION_SIGNING_KEY=signkey-prod-<hex> INNGEST_INTEGRATION_EVENT_KEY=<key> \
  bundle exec rake spec:integration

# Regenerate protobuf classes after updating proto/connect.proto (needs protoc)
bundle exec rake proto
```

The library is split so the execution engine doesn't depend on a transport:

- `Inngest::Executor` runs one call of a function: it replays memoized steps and reports new ones. Interrupts
  use `throw`/`catch`, so `rescue` in user code can't swallow them, while `ensure` blocks still run.
- `Inngest::Connect::Worker` implements the Connect protocol: the start request, handshake, heartbeats, lease
  extension, reply acks with HTTP flush fallback, gateway draining, reconnect with backoff, and graceful
  shutdown.
- `Inngest::Serve` is the HTTP transport: sync, introspection, signature checks (`Inngest::Signature`, with
  RFC 8785 canonical JSON in `Inngest::JCS`) and responses.

## Roadmap

- Streaming responses for serve mode, for platforms with short request timeouts
- `step.ai.infer` / `step.fetch`, checkpointing, realtime
