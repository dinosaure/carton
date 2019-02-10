(*
 * Copyright (c) 2013-2017 Romain Calascibetta <romain.calascibetta@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

module type BASE = sig
 type t

  val pp: t Fmt.t
  val compare: t -> t -> int
  val hash: t -> int
  val equal: t -> t -> bool

  module Set: Set.S with type elt = t
  module Map: Map.S with type key = t
end

module type CONVERTER = sig
  type t
  type buffer

  val to_t: buffer -> t
  val of_t: t -> buffer
end

module type INFLATE = sig
  type t
  type error
  type window

  val pp_error: error Fmt.t
  val pp: t Fmt.t

  val window_reset: window -> window
  val window: unit -> window

  val default: window -> t
  val eval: src:Cstruct.t -> dst:Cstruct.t -> t -> [ `Await of t | `Flush of t | `Error of (t * error) | `End of t ]
  val used_in: t -> int
  val used_out: t -> int
  val write: t -> int
  val flush: int -> int -> t -> t
  val refill: int -> int -> t -> t
end

module type DEFLATE = sig
  type t
  type error

  val pp_error: error Fmt.t

  val default: int -> t
  val flush: int -> int -> t -> t
  val no_flush: int -> int -> t -> t
  val finish: t -> t
  val used_in: t -> int
  val used_out: t -> int
  val eval: src:Cstruct.t -> dst:Cstruct.t -> t -> [ `Flush of t | `Await of t | `Error of (t * error) | `End of t ]
end

module type DIGEST = sig
  type t
  type ctx
  type buffer = Cstruct.t

  val init: unit -> ctx
  val feed: ctx -> buffer -> unit
  val get: ctx -> t

  val length: int
end

module type HASH = sig
  include BASE

  module Digest: DIGEST with type t = t

  val get: t -> int -> char

  val to_string: t -> string
  val of_string: string -> t

  type hex = string

  val to_hex: t -> hex
  val of_hex: hex -> t
end

module type MONAD = sig
  type +'a t

  val bind: 'a t -> ('a -> 'b t) -> 'b t
  val map: ('a -> 'b) -> 'a t -> 'b t
  val return: 'a -> 'a t
  val try_bind: (unit -> 'a t) -> ('a -> 'b t) -> (exn -> 'b t) -> 'b t
  val fail: exn -> 'a t
end

module type MR = sig
  type +'a m

  val map: ('a -> 'b m) -> 'a list -> 'b list m
  val fold: ('b -> 'a -> 'b m) -> 'b -> 'a list -> 'b m
end

module type MAPPER = sig
  type location
  type error
  type +'a m

  val pp_error: error Fmt.t
  val of_location: location -> (Cstruct.t, error) result m
  val length: location -> (int64, error) result m
  val map: location -> ?pos:int64 -> int -> (Cstruct.t, error) result m
  val close : location -> (unit, error) result m
end

module type OBJECT = sig
  type t

  val kind: int
  val name: string
end

module type CHECKSUM = sig
  type t = int32

  val digest: t -> ?off:int -> ?len:int -> Cstruct.t -> t
  val digestv: t -> Cstruct.t list -> t
  val digestc: t -> int -> t
  val digests: t -> ?off:int -> ?len:int -> Bytes.t -> t

  val pp: t Fmt.t

  val default: t
  val eq: t -> t -> bool
  val neq: t -> t -> bool
end
