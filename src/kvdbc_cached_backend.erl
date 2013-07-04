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

-define(CACHE_KEY_EXPIRATION, 2*60). % 2 min

start_link(BackendName, ProcessName) ->
    Mod = module_name(BackendName),
    Mod:start_link(BackendName, ProcessName).

put(BackendName, ProcessName, Table, Key, Value) ->
    count(ProcessName, put),
    Mod = module_name(BackendName),
    PutResponse = Mod:put(BackendName, ProcessName, Table, Key, Value),
    case PutResponse of
        ok ->
            set_cached_value(BackendName, mcd_key(ProcessName,Table,Key), Value);
        {ok, _} ->
            set_cached_value(BackendName, mcd_key(ProcessName,Table,Key), Value);
        _ -> no_dice
    end,
    PutResponse.

get(BackendName, ProcessName, Table, Key) -> 
    MCDKey = mcd_key(ProcessName, Table, Key),
    case get_cached_value(BackendName, MCDKey) of
        {ok, CachedValue} ->
            count(ProcessName, get_cached),
            {ok,CachedValue};  %% returns cached value
        _ ->  
            count(ProcessName, get),
            Mod = module_name(BackendName),
            GetResponse = Mod:get(BackendName, ProcessName, Table, Key),
            case GetResponse of
                {ok, Value} -> set_cached_value(BackendName, MCDKey, Value);
                _   -> no_dice
            end,
            GetResponse
    end.

delete(BackendName, ProcessName, Table, Key) ->
    count(ProcessName, delete),
    Mod = module_name(BackendName),
    DelResponse = Mod:delete(BackendName, ProcessName, Table, Key),
    case DelResponse  of
        ok -> delete_cached_value(BackendName, mcd_key(ProcessName, Table, Key));
        _ ->  no_dice
    end,
    DelResponse.  %% We return result of riak del opration in any case

list_keys(BackendName, ProcessName, Table) ->
    Mod = module_name(BackendName),
    Mod:list_keys(BackendName, ProcessName, Table).

list_buckets(BackendName, ProcessName) ->
    Mod = module_name(BackendName),
    Mod:list_buckets(BackendName, ProcessName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

module_name(BackendName) ->
    Config = kvdbc_cfg:backend_val(BackendName, config),
    proplists:get_value(wrapped_backend_module, Config).

cache_module(BackendName) ->
    Config = kvdbc_cfg:backend_val(BackendName, config),
    proplists:get_value(cache_module, Config).

mcd_key(ProcName, Table, Key) ->
    {?MODULE, ProcName, Table, Key}.

get_cached_value(BackendName, MCDKey) ->
    {Mod, ServerRef} = cache_module(BackendName),
    Mod:do(ServerRef, get, MCDKey).

set_cached_value(BackendName, MCDKey, Value) ->
    {Mod, ServerRef} = cache_module(BackendName),
    Mod:do(ServerRef, {set, 0, ?CACHE_KEY_EXPIRATION}, MCDKey, Value).

delete_cached_value(BackendName, MCDKey) ->
    {Mod, ServerRef} = cache_module(BackendName),
    Mod:do(ServerRef, delete, MCDKey).

count(ProcessName, Op) ->
    case kvdbc_cfg:metrics_module() of
        undefined -> nop;
        Mod ->
            CounterName = {<<".">>, list_to_binary("riakc." ++ atom_to_list(ProcessName) ++ "." ++ atom_to_list(Op)), $r},
            Mod:safely_notify(CounterName, 1, meter)
    end.
