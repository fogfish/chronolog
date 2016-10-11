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
%% @doc
%%    
-module(chronolog_file).
-include("chronolog.hrl").

-export([
   lookup/2
  ,ticker/2
  ,append/3
  ,stream/3
  ,encode/2
  ,decode/2
  ,mktag/3
  ,untag/3
  ,match/2
]).

%%
%% lookup ticker associated with internal 64-bit identifier
lookup(#chronolog{fd=FD}, {uid, Uid}) ->
   {ok, Urn} = dive:get(FD, <<$i, Uid/binary>>),
   uri:new(Urn).

%%
%% build ticker association with internal 64-bit identifier
ticker(#chronolog{fd=FD}, {urn, Schema, Path}=Urn) ->
   Key = <<$u, Schema/binary, $:, Path/binary>>,
   case dive:get(FD, Key) of
      {error, not_found} ->
         Uid = uid:encode(uid:l()),
         %% definition of urn have to be serialized due concurrency 
         dive:apply(FD, 
            fun() -> 
               case dive:get(FD, Key) of
                  {error, not_found} ->
                     ok  = dive:put_(FD, Key, Uid),
                     ok  = dive:put_(FD, <<$i, Uid/binary>>, uri:s(Urn)),
                     {ok, {uid, Uid}};                     
                  {ok, Val} ->
                     {ok, {uid, Val}}
               end
            end
         );
      {ok, Val} ->
         {ok, {uid, Val}}
   end;

ticker(_, '_') ->
   {ok, {uid, uid:encode(uid:l())}};

ticker(_, {uid, _}=Uid) ->
   {ok, Uid}.


%%
%% append value to time-series
append(#chronolog{fd=FD}, {uid, Uid}, {T, X}) ->
   dive:put_(dive:ioctl(nocache, FD), <<$x, Uid/binary, T/binary>>, X).

%%
%%
stream(#chronolog{fd=FD}=File, {uid, Uid}, {Ta, Tb}) ->
   A = <<$x, Uid/binary, (encode_key(File, Ta))/binary>>,
   B = <<$x, Uid/binary, (encode_key(File, Tb))/binary>>,
   stream:map(
      fun(Val) -> decode(File, Val) end,
      stream:takewhile(
         fun({Key, _}) -> Key =< B end,
         dive:match(FD, {'>=', A})
      )
   ).
   
%%
%% encode series to internal storage format
encode(FD, {{_, _, _}=T, X}) ->
   {encode_key(FD, T), encode_val(FD, X)};
encode(FD, X) ->
   encode(FD, {os:timestamp(), X}).

%% @todo: optimal encoding of chronon (var int - add support to scalar)
encode_key(#chronolog{chronon={0,0,1}}, {A, B, C}) ->
   <<A:24, B:20, C:20>>.

encode_val(_, X)
 when is_integer(X) ->
   %% @todo: var int as value (?)
   <<$i, X:32>>;
encode_val(_, X)
 when is_float(X) ->
   <<$f, X:64/float>>;
encode_val(_, X)
 when is_binary(X) ->
   <<$b, X/binary>>;
encode_val(_, {uid, _, _} = X) ->
   <<$u, (uid:encode(X))/binary>>;
encode_val(_, {uid, _, _, _} = X) ->
   <<$u, (uid:encode(X))/binary>>;
encode_val(_, {uid, X}) ->
   <<$t, X/binary>>.

%%
%%
decode(FD, {Key, Val}) ->
   {decode_key(FD, Key), decode_val(FD, Val)}.

decode_key(#chronolog{chronon={0,0,1}}, <<$x, _:64, A:24, B:20, C:20>>) ->
   {A, B, C}.

decode_val(_, <<$i, X:32>>) ->
   X;
decode_val(_, <<$f, X:64/float>>) ->
   X;
decode_val(_, <<$b, X/binary>>) ->
   X;
decode_val(_, <<$u, X/binary>>) ->
   uid:decode(X);
decode_val(_, <<$t, X/binary>>) ->
   {uid, X}.

%%
%%
mktag(#chronolog{fd=FD}, {uid, Uid}, Tag) ->
   dive:put_(FD, <<$t, Tag/binary, $0, Uid/binary>>, <<>>).   

%%
%%
untag(#chronolog{fd=FD}, {uid, Uid}, Tag) ->
   dive:remove_(FD, <<$t, Tag/binary, $0, Uid/binary>>).   

%%
%%
match(#chronolog{fd=FD}, Prefix) ->
   stream:map(
      fun({Key, _}) -> 
         [<<$t, Tag/binary>>, Uid] = binary:split(Key, <<$0>>),
         {Tag, {uid, Uid}}
      end,
      dive:match(FD, {'~', <<$t, Prefix/binary>>})
   ).


