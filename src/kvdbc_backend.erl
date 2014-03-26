-module(kvdbc_backend).

-type error() :: {'error', term()}.
-type instance_name() :: atom().
-type table() :: binary().
-type key() :: binary().
-type value() :: term().
-type opts() :: term().

-callback start_link(instance_name()) -> term().
-callback get(instance_name(), table(), key(), opts()) -> error() | {'ok', value()}.
-callback put(instance_name(), table(), key(), value(), opts()) -> error() | 'ok'.
-callback delete(instance_name(), table(), key(), opts()) -> error() | 'ok'.
-callback list_keys(instance_name(), Table :: table(), opts()) -> error() | {'ok', [key()]}.
-callback list_buckets(instance_name(), opts()) -> error() | {'ok', [table()]}.
