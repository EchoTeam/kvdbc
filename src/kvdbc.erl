%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc).

-export([
        get/2,
        get/3,
        put/3,
        put/4,
        delete/2,
        delete/3,
        list_buckets/0,
        list_buckets/1,
        list_keys/1,
        list_keys/2
    ]).

-ifdef(TEST).
-export([
    error_reason_to_string/1,
]).
-endif.

-export_type([
    errors/0,
    error/0,
    process_name/0,
    instance_name/0,
    table/0,
    key/0,
    value/0
]).


-define(DEFAULT_BACKEND_INSTANCE, default).

-type kvdbc_errors() :: 'service_is_not_available'.
-type errors() :: kvdbc_riak_backend:errors() | kvdbc_errors().
-type error() :: kvdbc_riak_backend:error() | {'error', kvdbc_errors()}.
-type process_name() :: kvdbc_riak_backend:process_name().
-type instance_name() :: atom().
-type table() :: binary().
-type key() :: binary().
-type value() :: term().

-spec put(Table :: table(), Key :: key(), Value :: value()) -> error() | 'ok'.
put(Table, Key, Value) ->
    put(?DEFAULT_BACKEND_INSTANCE, Table, Key, Value).

-spec put(InstanceName :: instance_name(), Table :: table(), Key :: key(), Value :: value()) -> error() | 'ok'.
put(InstanceName, Table, Key, Value) ->
    call(put, InstanceName, [Table, Key, Value]).

-spec get(Table :: table(), Key :: key()) -> error() | {'ok', value()}.
get(Table, Key) ->
    get(?DEFAULT_BACKEND_INSTANCE, Table, Key).

-spec get(InstanceName :: instance_name(), Table :: table(), Key :: key()) -> error() | {'ok', value()}.
get(InstanceName, Table, Key) ->
    call(get, InstanceName, [Table, Key]).

-spec delete(Table :: table(), Key :: key()) -> error() | 'ok'.
delete(Table, Key) ->
    delete(?DEFAULT_BACKEND_INSTANCE, Table, Key).

-spec delete(InstanceName :: instance_name(), Table :: table(), Key :: key()) -> error() | 'ok'.
delete(InstanceName, Table, Key) ->
    call(delete, InstanceName, [Table, Key]).

-spec list_keys(Table :: table()) -> error() | {'ok', [key()]}.
list_keys(Table) -> list_keys(?DEFAULT_BACKEND_INSTANCE, Table).

-spec list_keys(InstanceName :: instance_name(), Table :: table()) -> error() | {'ok', [key()]}.
list_keys(InstanceName, Table) ->
    call(list_keys, InstanceName, [Table]).

-spec list_buckets() -> error() | {'ok', [table()]}.
list_buckets() ->
    list_buckets(?DEFAULT_BACKEND_INSTANCE).

-spec list_buckets(InstanceName :: instance_name()) -> error() | {'ok', [table()]}.
list_buckets(InstanceName) ->
    call(list_buckets, InstanceName, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

call(Func, InstanceName, Args) ->
    ProcessName = kvdbc_cfg:backend_val(InstanceName, process_name),
    Module = kvdbc_cfg:backend_val(InstanceName, callback_module),
    case handler_call(Module, Func, [InstanceName, ProcessName | Args]) of
        {error, _Reason} = Error ->
            log_error(Error, Module, Func, InstanceName, ProcessName, Args),
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

log_error({error, notfound}, _Module, _Func, _InstanceName, _ProcessName, _Args) -> nop;
log_error({error, Reason} = Error, Module, Func, InstanceName, ProcessName, Args) ->
    save_error_stats(Reason, InstanceName),
    LogFormat = "Module: ~p; Func: ~p; InstanceName: ~p; ProcessName: ~p; Args: ~p~nError:~n~p",
    LogData = [Module, Func, InstanceName, ProcessName, Args, Error],
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
    "general".

safe_string(S) ->
    re:replace(S, "\\W", "_", [global, {return, list}]).
