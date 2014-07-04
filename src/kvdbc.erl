%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc).

-export([
        create_bucket/3,
        get/3,
        get/4,
        put/4,
        put/5,
        delete/3,
        delete/4,
        list_buckets/1,
        list_buckets/2,
        list_keys/2,
        list_keys/3
    ]).

-ifdef(TEST).
-export([
    error_reason_to_string/1
]).
-endif.

-export_type([
    errors/0,
    error/0,
    instance_name/0,
    table/0,
    key/0,
    value/0,
    opts/0
]).


-type kvdbc_errors() :: 'service_is_not_available'.
-type errors() :: kvdbc_riak_backend:errors() | kvdbc_errors().
-type error() :: kvdbc_riak_backend:error() | {'error', kvdbc_errors()}.
-type instance_name() :: atom().
-type table() :: binary().
-type key() :: binary().
-type value() :: term().
-type opts() :: term().

-spec create_bucket(instance_name(), table(), opts()) -> error() | {'ok', table()}.
create_bucket(InstanceName, BucketName, Opts) ->
    call(create_bucket, InstanceName, [BucketName, Opts]).

-spec put(instance_name(), table(), key(), value()) -> error() | 'ok'.
put(InstanceName, Table, Key, Value) ->
    put(InstanceName, Table, Key, Value, []).

-spec put(instance_name(), table(), key(), value(), opts()) -> error() | 'ok'.
put(InstanceName, Table, Key, Value, Opts) ->
    call(put, InstanceName, [Table, Key, Value, Opts]).

-spec get(instance_name(), table(), key()) -> error() | {'ok', value()}.
get(InstanceName, Table, Key) ->
    get(InstanceName, Table, Key, []).

-spec get(instance_name(), table(), key(), opts()) -> error() | {'ok', value()}.
get(InstanceName, Table, Key, Opts) ->
    call(get, InstanceName, [Table, Key, Opts]).

-spec delete(instance_name(), table(), key()) -> error() | 'ok'.
delete(InstanceName, Table, Key) ->
    delete(InstanceName, Table, Key, []).

-spec delete(instance_name(), table(), key(), opts()) -> error() | 'ok'.
delete(InstanceName, Table, Key, Opts) ->
    call(delete, InstanceName, [Table, Key, Opts]).

-spec list_keys(instance_name(), table()) -> error() | {'ok', [key()]}.
list_keys(InstanceName, Table) ->
    list_keys(InstanceName, Table, []).

-spec list_keys(instance_name(), table(), opts()) -> error() | {'ok', [key()]}.
list_keys(InstanceName, Table, Opts) ->
    call(list_keys, InstanceName, [Table, Opts]).

-spec list_buckets(instance_name()) -> error() | {'ok', [table()]}.
list_buckets(InstanceName) ->
    call(list_buckets, InstanceName, [[]]).

-spec list_buckets(instance_name(), opts()) -> error() | {'ok', [table()]}.
list_buckets(InstanceName, Opts) ->
    call(list_buckets, InstanceName, [Opts]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

call(Func, InstanceName, Args) ->
    Module = kvdbc_cfg:backend_val(InstanceName, callback_module),
    case handler_call(Module, Func, [InstanceName | Args]) of
        {error, _Reason} = Error ->
            log_error(Error, Module, Func, InstanceName, Args),
            Error;
        Any ->
            Any
    end.

handler_call(Module, Func, Args) ->
    try
        erlang:apply(Module, Func, Args)
    catch
        exit:{noproc, {gen_server, call, _Params}} ->
            {error, service_is_not_available}
    end.

log_error({error, notfound}, _Module, _Func, _InstanceName, _Args) -> nop;
log_error({error, Reason} = Error, Module, Func, InstanceName, Args) ->
    save_error_stats(Reason, InstanceName),
    LogFormat = "Module: ~p; Func: ~p; InstanceName: ~p; Args: ~p~nError:~n~p",
    LogData = [Module, Func, InstanceName, Args, Error],
    lager:error(LogFormat, LogData).

save_error_stats(Reason, InstanceName) ->
    case kvdbc_cfg:metrics_module() of
        undefined -> nop;
        Mod ->
            Domain = atom_to_list(InstanceName),
            CounterName = "kvdbc.errors." ++ safe_string(error_reason_to_string(Reason)) ++ ".segments." ++ Domain,
            Mod:safely_notify(CounterName, 1, meter)
    end.

error_reason_to_string(Reason) when is_atom(Reason) ->
    atom_to_list(Reason);
error_reason_to_string(_Reason) ->
    "unknown".

safe_string(S) ->
    re:replace(S, "\\W", "_", [global, {return, list}]).
