module type KIND =
sig
  module A: S.OBJECT
  module B: S.OBJECT
  module C: S.OBJECT
  module D: S.OBJECT

  type t = A | B | C | D

  val to_int: t -> int
  val to_bin: t -> int
  val pp: t Fmt.t
end

module Kind
    (A: S.OBJECT)
    (B: S.OBJECT)
    (C: S.OBJECT)
    (D: S.OBJECT)
  : KIND with module A = A
          and module B = B
          and module C = C
          and module D = D

module type ENTRY =
sig
  module A: S.OBJECT
  module B: S.OBJECT
  module C: S.OBJECT
  module D: S.OBJECT

  module Hash: S.HASH

  module Kind: KIND
    with module A := A
     and module B := B
     and module C := C
     and module D := D

  type t
  type source = From of Hash.t | None

  val pp: t Fmt.t
  val pp_source: source Fmt.t
  val hash: string -> int
  val make:
    Hash.t ->
    ?name:string ->
    ?preferred:bool -> ?delta:source -> Kind.t -> int64 -> t

  val id: t -> Hash.t
  val kind: t -> Kind.t
  val preferred: t -> bool
  val delta: t -> source
  val length: t -> int64

  val with_delta: t -> source -> t
  val with_preferred: t -> bool -> t
  val name: t -> string -> t

  val compare: t -> t -> int

  val topological_sort: t list -> t list
end

module Entry
    (A: S.OBJECT)
    (B: S.OBJECT)
    (C: S.OBJECT)
    (D: S.OBJECT)
    (Hash: S.HASH)
    (Kind: KIND with module A := A
                 and module B := B
                 and module C := C
                 and module D := D)
  : ENTRY with module A = A
           and module B = B
           and module C = C
           and module D = D
           and module Kind := Kind
           and module Hash = Hash

module type H =
sig
  module Hash: S.HASH

  type error

  val pp_error: error Fmt.t

  type t
  type reference =
    | Offset of int64
    | Hash of Hash.t

  val pp: t Fmt.t
  val default: reference -> int -> int -> Rabin.t list -> t
  val refill: int -> int -> t -> t
  val flush: int -> int -> t -> t
  val finish: t -> t

  val eval:
    Cstruct.t ->
    Cstruct.t ->
    t -> [ `Await of t | `End of t | `Error of t * error | `Flush of t ]

  val used_in: t -> int
  val used_out: t -> int
end

module Hunk (Hash : S.HASH)
  : H with module Hash = Hash

module type DELTA =
sig
  module A: S.OBJECT
  module B: S.OBJECT
  module C: S.OBJECT
  module D: S.OBJECT

  module Hash: S.HASH

  module Kind: KIND
    with module A := A
     and module B := B
     and module C := C
     and module D := D

  module Entry: ENTRY
    with module A := A
     and module B := B
     and module C := C
     and module D := D
     and module Hash := Hash
     and module Kind := Kind

  module Monad : S.MONAD

  type t = { mutable delta : delta; }
  and delta =
    | Z
    | S of { length: int
           ; depth: int
           ; hunks: Rabin.t list
           ; src: t
           ; src_length: int64
           ; src_hash: Hash.t
           ; }

  type error = Invalid_hash of Hash.t

  val pp_error: error Fmt.t

  val deltas:
    ?memory:bool ->
    Entry.t list ->
    (Hash.t -> Cstruct.t option Monad.t) ->
    (Entry.t -> bool) ->
    int -> int -> ((Entry.t * t) list, error) result Monad.t
end

module Delta
    (A: S.OBJECT)
    (B: S.OBJECT)
    (C: S.OBJECT)
    (D: S.OBJECT)
    (Hash: S.HASH)
    (Kind: KIND with module A := A
                 and module B := B
                 and module C := C
                 and module D := D)
    (Entry: ENTRY with module A := A
                   and module B := B
                   and module C := C
                   and module D := D
                   and module Kind := Kind
                   and module Hash := Hash)
    (Monad: S.MONAD)
    (Mr: S.MR with type +'a m = 'a Monad.t)
  : DELTA with module A = A
           and module B = B
           and module C = C
           and module D = D
           and module Hash = Hash
           and module Kind := Kind
           and module Entry := Entry
           and module Monad = Monad

module type P =
sig
  module A: S.OBJECT
  module B: S.OBJECT
  module C: S.OBJECT
  module D: S.OBJECT

  module Hash: S.HASH
  module Checksum: S.CHECKSUM
  module Deflate: S.DEFLATE

  module Kind: KIND
    with module A := A
     and module B := B
     and module C := C
     and module D := D

  module Entry: ENTRY
    with module A := A
     and module B := B
     and module C := C
     and module D := D
     and module Kind := Kind
     and module Hash := Hash

  module Delta: DELTA
    with module A := A
     and module B := B
     and module C := C
     and module D := D
     and module Hash := Hash
     and module Kind := Kind
     and module Entry := Entry

  module Hunk: H
    with module Hash := Hash

  module Radix: Radix.S with type key = Hash.t

  type error =
    | Deflate_error of Deflate.error
    | Invalid_hash of Hash.t

  type t

  val pp_error: error Fmt.t

  val used_out: t -> int
  val used_in: t -> int

  val flush: int -> int -> t -> t
  val refill: int -> int -> t -> t
  val finish: t -> t

  val expect: t -> Hash.t
  val idx: t -> (Checksum.t * int64) Radix.t

  val default: Cstruct.t -> (Entry.t * Delta.t) list -> t

  val eval :
    Cstruct.t ->
    Cstruct.t ->
    t ->
    [ `Await of t | `End of t * Hash.t | `Error of t * error | `Flush of t ]
end

module Pack
    (A: S.OBJECT)
    (B: S.OBJECT)
    (C: S.OBJECT)
    (D: S.OBJECT)
    (Hash: S.HASH)
    (Checksum: S.CHECKSUM)
    (Deflate: S.DEFLATE)
    (Kind: KIND with module A := A
                 and module B := B
                 and module C := C
                 and module D := D)
    (Entry: ENTRY with module A := A
                   and module B := B
                   and module C := C
                   and module D := D
                   and module Kind := Kind
                   and module Hash := Hash)
    (Delta : DELTA with module A := A
                    and module B := B
                    and module C := C
                    and module D := D
                    and module Hash := Hash
                    and module Kind := Kind
                    and module Entry := Entry)
    (Hunk: H with module Hash := Hash)
  : P with module A = A
       and module B = B
       and module C = C
       and module D = D
       and module Hash = Hash
       and module Checksum = Checksum
       and module Deflate = Deflate
       and module Kind := Kind
       and module Entry := Entry
       and module Delta := Delta
       and module Hunk := Hunk

module Stream
    (A: S.OBJECT)
    (B: S.OBJECT)
    (C: S.OBJECT)
    (D: S.OBJECT)
    (Hash: S.HASH)
    (Checksum: S.CHECKSUM)
    (Deflate: S.DEFLATE)
    (Monad: S.MONAD)
    (Mr: S.MR with type +'a m = 'a Monad.t) : sig
  include P
end with module A = A
     and module B = B
     and module C = C
     and module D = D
     and module Hash = Hash
     and module Checksum = Checksum
     and module Deflate = Deflate
     and module Delta.Monad = Monad
