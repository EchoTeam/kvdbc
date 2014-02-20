%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc_cached_backend).
-behaviour(kvdbc_backend).

-export([
    start_link/1,
    get/4,
    put/5,
    delete/4,
    list_buckets/2,
    list_keys/3
]).

-type error() :: kvdbc:error().
-type process_name() :: kvdbc:process_name().
-type instance_name() :: kvdbc:instance_name().
-type table() :: kvdbc:table().
-type key() :: kvdbc:key().
-type value() :: kvdbc:value().
-type opts() :: kvdbc:opts().

-type mcdkey() :: {instance_name(), process_name(), table(), key()}.
-type mcd_answer() :: {'error', term()} | {'ok', term()}.

-define(CACHE_KEY_EXPIRATION, 2*60). % 2 min

start_link(_InstanceName) ->
    ignore.

-spec put(instance_name(), table(), key(), value(), opts()) -> error() | 'ok'.
put(InstanceName, Table, Key, Value, Opts) ->
    count(InstanceName, put),
    PutResponse = call_backend(InstanceName, put, [Table, Key, Value, Opts]),
    case PutResponse of
        ok ->
            set_cached_value(InstanceName, mcd_key(InstanceName,Table,Key), Value);
        {ok, _} ->
            set_cached_value(InstanceName, mcd_key(InstanceName,Table,Key), Value);
        _ -> no_dice
    end,
    PutResponse.

-spec get(instance_name(), table(), key(), opts()) -> error() | {'ok', value()}.
get(InstanceName, Table, Key, Opts) -> 
    MCDKey = mcd_key(InstanceName, Table, Key),
    case get_cached_value(InstanceName, MCDKey) of
        {ok, CachedValue} ->
            count(InstanceName, get_cached),
            {ok,CachedValue};  %% returns cached value
        _ ->  
            count(InstanceName, get),
            GetResponse = call_backend(InstanceName, get, [Table, Key, Opts]),
            case GetResponse of
                {ok, Value} -> set_cached_value(InstanceName, MCDKey, Value);
                _   -> no_dice
            end,
            GetResponse
    end.

-spec delete(instance_name(), table(), key(), opts()) -> error() | 'ok'.
delete(InstanceName, Table, Key, Opts) ->
    count(InstanceName, delete),
    DelResponse = call_backend(InstanceName, delete, [Table, Key, Opts]),
    case DelResponse  of
        ok -> delete_cached_value(InstanceName, mcd_key(InstanceName, Table, Key));
        _ ->  no_dice
    end,
    DelResponse.  %% We return result of riak del opration in any case

-spec list_keys(instance_name(), table(), opts()) -> error() | {'ok', [key()]}.
list_keys(InstanceName, Table, Opts) ->
    call_backend(InstanceName, list_keys, [Table, Opts]).

-spec list_buckets(instance_name(), opts()) -> error() | {'ok', [table()]}.
list_buckets(InstanceName, Opts) ->
    call_backend(InstanceName, list_buckets, [Opts]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec module_name(InstanceName :: instance_name()) -> atom().
module_name(InstanceName) ->
    BackendInstance = kvdbc_cfg:config_val(InstanceName, backend_instance),
    kvdbc_cfg:instance_val(BackendInstance, callback_module).

-spec cache_module(InstanceName :: instance_name()) -> {atom(), atom()}.
cache_module(InstanceName) ->
    Mod = kvdbc_cfg:config_val(InstanceName, cache_module),
    ServerRef = kvdbc_cfg:config_val(InstanceName, cache_serverref),
    {Mod, ServerRef}.

-spec mcd_key(InstanceName :: instance_name(), Table :: table(), Key :: key()) -> mcdkey().
mcd_key(InstanceName, Table, Key) ->
    {?MODULE, InstanceName, Table, Key}.

-spec get_cached_value(InstanceName :: instance_name(), MCDKey :: mcdkey()) -> mcd_answer().
get_cached_value(InstanceName, MCDKey) ->
    {Mod, ServerRef} = cache_module(InstanceName),
    Mod:get(ServerRef, MCDKey).

-spec set_cached_value(InstanceName :: instance_name(), MCDKey :: mcdkey(), Value :: value()) -> mcd_answer().
set_cached_value(InstanceName, MCDKey, Value) ->
    {Mod, ServerRef} = cache_module(InstanceName),
    Mod:set(ServerRef, MCDKey, Value, ?CACHE_KEY_EXPIRATION).

-spec delete_cached_value(InstanceName :: instance_name(), MCDKey :: mcdkey()) -> mcd_answer().
delete_cached_value(InstanceName, MCDKey) ->
    {Mod, ServerRef} = cache_module(InstanceName),
    Mod:delete(ServerRef, MCDKey).

count(InstanceName, Op) ->
    case kvdbc_cfg:metrics_module() of
        undefined -> nop;
        Mod ->
            CounterName = lists:flatten(io_lib:format("kvdbc.~s.~s.~s",
                    [?MODULE, InstanceName, Op])),
            Mod:safely_notify(CounterName, 1, meter)
    end.
    
call_backend(InstanceName, Op, Args) ->
    BackendInstance = kvdbc_cfg:config_val(InstanceName, backend_instance),
    Module = kvdbc_cfg:instance_val(BackendInstance, callback_module),
    apply(Module, Op, [BackendInstance | Args]).
