-module(kvdbc_backend).

-type error() :: {'error', term()}.
-type process_name() :: atom().
-type instance_name() :: atom().
-type table() :: binary().
-type key() :: binary().
-type value() :: term().

-callback start_link(instance_name(), process_name()) -> term().
-callback get(instance_name(), process_name(), table(), key()) -> error() | {'ok', value()}.
-callback put(instance_name(), process_name(), table(), key(), value()) -> error() | 'ok'.
-callback delete(instance_name(), process_name(), table(), key()) -> error() | 'ok'.
-callback list_keys(instance_name(), process_name(), Table :: table()) -> error() | {'ok', [key()]}.
-callback list_buckets(instance_name(), process_name()) -> error() | {'ok', [table()]}.
