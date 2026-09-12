CREATE TABLE runs (
 run_id text PRIMARY KEY, dataset_hash text NOT NULL, config_hash text NOT NULL,
 config_json jsonb NOT NULL, input_path text NOT NULL, build_id text NOT NULL,
 status text NOT NULL CHECK(status IN ('RUNNING','COMPLETE')), created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE checkpoints (
 run_id text PRIMARY KEY REFERENCES runs, next_index bigint NOT NULL CHECK(next_index>0),
 snapshot_version integer NOT NULL CHECK(snapshot_version=1), state jsonb NOT NULL, finalized boolean NOT NULL
);
CREATE TABLE event_outcomes (
 run_id text REFERENCES runs, physical_index bigint CHECK(physical_index>0), sequence bigint,
 type text NOT NULL, disposition text NOT NULL CHECK(disposition IN ('ACCEPTED','IGNORED','QUARANTINED')), reason text NOT NULL,
 PRIMARY KEY(run_id,physical_index)
);
CREATE TABLE orders (
 run_id text REFERENCES runs, order_id text, symbol text NOT NULL, side text NOT NULL CHECK(side IN ('BUY','SELL')),
 quantity integer NOT NULL CHECK(quantity>0), filled integer NOT NULL CHECK(filled>=0 AND filled<=quantity),
 status text NOT NULL CHECK(status IN ('REJECTED','ACCEPTED','FILLED','PARTIALLY_FILLED','CANCELLED')),
 trigger_index bigint NOT NULL CHECK(trigger_index>0), data jsonb NOT NULL, PRIMARY KEY(run_id,order_id)
);
CREATE TABLE order_events (
 run_id text, order_id text, transition_index integer CHECK(transition_index>=0),
 data jsonb NOT NULL, PRIMARY KEY(run_id,order_id,transition_index),
 FOREIGN KEY(run_id,order_id) REFERENCES orders
);
CREATE TABLE fills (
 run_id text, order_id text, fill_index integer CHECK(fill_index>=0), execution_index bigint NOT NULL CHECK(execution_index>0),
 price_ticks bigint NOT NULL CHECK(price_ticks>0), quantity integer NOT NULL CHECK(quantity>0), fee_ticks bigint NOT NULL CHECK(fee_ticks>=0),
 data jsonb NOT NULL, PRIMARY KEY(run_id,order_id,fill_index), FOREIGN KEY(run_id,order_id) REFERENCES orders
);
CREATE TABLE positions (
 run_id text REFERENCES runs, symbol text, quantity integer NOT NULL CHECK(quantity>=0), cost_ticks bigint NOT NULL CHECK(cost_ticks>=0),
 PRIMARY KEY(run_id,symbol)
);
CREATE TABLE account_state (
 run_id text PRIMARY KEY REFERENCES runs, cash_ticks bigint NOT NULL CHECK(cash_ticks>=0), fees_ticks bigint NOT NULL CHECK(fees_ticks>=0)
);
CREATE INDEX orders_symbol_history ON orders(run_id,symbol,trigger_index);
CREATE INDEX fills_execution_history ON fills(run_id,execution_index);
