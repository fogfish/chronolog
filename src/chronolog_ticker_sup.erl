%% @doc
%%   
-module(chronolog_ticker_sup).
-behaviour(supervisor).

-export([
   start_link/0
  ,init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, temporary, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, temporary, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, temporary, 5000, Type, dynamic}).


%%-----------------------------------------------------------------------------
%%
%% supervisor
%%
%%-----------------------------------------------------------------------------

start_link() ->
   supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_) -> 
   {ok,
      {
         {simple_one_for_one, 100000, 1},  
         [
            ?CHILD(worker, chronolog_ticker)
         ]
      }
   }.



