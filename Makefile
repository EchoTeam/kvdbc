.PHONY: test

all:
	./rebar compile

run: all
	erl -pa deps/*/ebin ebin

test: all
	./rebar eunit skip_deps=true
	./rebar ct skip_deps=true
