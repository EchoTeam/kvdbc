%%% vim: set ts=4 sts=4 sw=4 expandtab:

-module(kvdbc_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

error_reason_to_string_test() ->
    [begin
        ?assertEqual(Expectation, kvdbc:error_reason_to_string(Reason))
    end || {Expectation, Reason} <- [
        {"timeout", timeout},
        {"undefined", undefined},
        {"unknown", {a, b}}
    ]].

-endif.
