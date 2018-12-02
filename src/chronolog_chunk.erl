%%
%%   Copyright (c) 2011 - 2015, Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
-module(chronolog_chunk).
-behaviour(pipe).

-export([
   start_link/2
,  init/1
,  free/2
,  handle/3
]).

-record(state, {
   heap = undefined :: datum:tree()
}).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Ns, Chunk) ->
   pipe:start_link({via, pns, {urn, Ns, Chunk}}, ?MODULE, [], []).

init([]) ->
   {ok, handle,
      #state{
         heap = rbtree:new()
      }
   }.

free(_, _) ->
   ok.

%%%----------------------------------------------------------------------------   
%%%
%%% state machine
%%%
%%%----------------------------------------------------------------------------   

handle({put, _, Stream}, _, #state{heap = Heap} = State) ->
   {reply, ok, 
      State#state{
         heap = lists:foldl(fun({T, X}, Acc) -> rbtree:insert(T, X, Acc) end, Heap, Stream)
      }
   };

handle({get, {_, _, undefined, undefined}}, _, #state{heap = Heap} = State) ->
   {reply
   ,  {ok, rbtree:list(Heap)}
   ,  State
   };

handle({get, {_, _, A, undefined}}, _, #state{heap = Heap} = State) ->
   {reply
   ,  {ok, rbtree:list(rbtree:dropwhile(fun({X, _}) -> X < A end, Heap))}
   ,  State
   };

handle({get, {_, _, undefined, B}}, _, #state{heap = Heap} = State) ->
   {reply
   ,  {ok, rbtree:list(rbtree:takewhile(fun({X, _}) -> X =< B end, Heap))}
   ,  State
   };

handle({get, {_, _, A, B}}, _, #state{heap = Heap} = State) ->
   {reply
   ,  {ok, rbtree:list(rbtree:takewhile(fun({X, _}) -> X =< B end, rbtree:dropwhile(fun({X, _}) -> X  < A end, Heap)))}
   ,  State
   }.
