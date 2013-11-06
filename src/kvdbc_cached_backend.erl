%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc_cached_backend).
-behaviour(kvdbc_backend).

-export([
    start_link/2,
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

-type mcdkey() :: {instance_name(), process_name(), table(), key()}.
-type mcd_answer() :: {'error', term()} | {'ok', term()}.

-define(CACHE_KEY_EXPIRATION, 2*60). % 2 min

start_link(InstanceName, ProcessName) ->
    Mod = module_name(InstanceName),
    Mod:start_link(InstanceName, ProcessName).

-spec put(InstanceName :: instance_name(), ProcessName :: process_name(), Table :: table(), Key :: key(), Value :: value()) -> error() | 'ok'.
put(InstanceName, ProcessName, Table, Key, Value) ->
    count(ProcessName, put),
    Mod = module_name(InstanceName),
    PutResponse = Mod:put(InstanceName, ProcessName, Table, Key, Value),
    case PutResponse of
        ok ->
            set_cached_value(InstanceName, mcd_key(ProcessName,Table,Key), Value);
        {ok, _} ->
            set_cached_value(InstanceName, mcd_key(ProcessName,Table,Key), Value);
        _ -> no_dice
    end,
    PutResponse.

-spec get(InstanceName :: instance_name(), ProcessName :: process_name(), Table :: table(), Key :: key()) -> error() | {'ok', value()}.
get(InstanceName, ProcessName, Table, Key) -> 
    MCDKey = mcd_key(ProcessName, Table, Key),
    case get_cached_value(InstanceName, MCDKey) of
        {ok, CachedValue} ->
            count(ProcessName, get_cached),
            {ok,CachedValue};  %% returns cached value
        _ ->  
            count(ProcessName, get),
            Mod = module_name(InstanceName),
            GetResponse = Mod:get(InstanceName, ProcessName, Table, Key),
            case GetResponse of
                {ok, Value} -> set_cached_value(InstanceName, MCDKey, Value);
                _   -> no_dice
            end,
            GetResponse
    end.

-spec delete(InstanceName :: instance_name(), ProcessName :: process_name(), Table :: table(), Key :: key()) -> error() | 'ok'.
delete(InstanceName, ProcessName, Table, Key) ->
    count(ProcessName, delete),
    Mod = module_name(InstanceName),
    DelResponse = Mod:delete(InstanceName, ProcessName, Table, Key),
    case DelResponse  of
        ok -> delete_cached_value(InstanceName, mcd_key(ProcessName, Table, Key));
        _ ->  no_dice
    end,
    DelResponse.  %% We return result of riak del opration in any case

-spec list_keys(InstanceName :: instance_name(), ProcessName :: process_name(), Table :: table()) -> error() | {'ok', [key()]}.
list_keys(InstanceName, ProcessName, Table) ->
    Mod = module_name(InstanceName),
    Mod:list_keys(InstanceName, ProcessName, Table).

-spec list_buckets(InstanceName :: instance_name(), ProcessName :: process_name()) -> error() | {'ok', [table()]}.
list_buckets(InstanceName, ProcessName) ->
    Mod = module_name(InstanceName),
    Mod:list_buckets(InstanceName, ProcessName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec module_name(InstanceName :: instance_name()) -> atom().
module_name(InstanceName) ->
    Config = kvdbc_cfg:backend_val(InstanceName, config),
    proplists:get_value(wrapped_backend_module, Config).

-spec cache_module(InstanceName :: instance_name()) -> {atom(), atom()}.
cache_module(InstanceName) ->
    Config = kvdbc_cfg:backend_val(InstanceName, config),
    proplists:get_value(cache_module, Config).

-spec mcd_key(ProcName :: process_name(), Table :: table(), Key :: key()) -> mcdkey().
mcd_key(ProcName, Table, Key) ->
    {?MODULE, ProcName, Table, Key}.

-spec get_cached_value(InstanceName :: instance_name(), MCDKey :: mcdkey()) -> mcd_answer().
get_cached_value(InstanceName, MCDKey) ->
    {Mod, ServerRef} = cache_module(InstanceName),
    Mod:do(ServerRef, get, MCDKey).

-spec set_cached_value(InstanceName :: instance_name(), MCDKey :: mcdkey(), Value :: value()) -> mcd_answer().
set_cached_value(InstanceName, MCDKey, Value) ->
    {Mod, ServerRef} = cache_module(InstanceName),
    Mod:do(ServerRef, {set, 0, ?CACHE_KEY_EXPIRATION}, MCDKey, Value).

-spec delete_cached_value(InstanceName :: instance_name(), MCDKey :: mcdkey()) -> mcd_answer().
delete_cached_value(InstanceName, MCDKey) ->
    {Mod, ServerRef} = cache_module(InstanceName),
    Mod:do(ServerRef, delete, MCDKey).

count(ProcessName, Op) ->
    case kvdbc_cfg:metrics_module() of
        undefined -> nop;
        Mod ->
            CounterName = lists:flatten(io_lib:format("kvdbc.~s.~s.~s",
                    [?MODULE, ProcessName, Op])),
            Mod:safely_notify(CounterName, 1, meter)
    end.
