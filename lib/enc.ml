open Sigs

type 'uid entry =
  { uid : 'uid
  ; kind : kind
  ; length : int
  ; preferred : bool
  ; hash : int
  ; delta : 'uid delta }
and 'uid delta = From of 'uid | Zero

let kind_to_int = function
  | `A -> 0 | `B -> 1 | `C -> 2 | `D -> 3
let bool_to_int = function
  | false -> 0 | true -> 1

let compare_entry a b =
  if kind_to_int a.kind > kind_to_int b.kind then (-1)
  else if kind_to_int a.kind < kind_to_int b.kind then 1
  else if a.hash > b.hash then (-1)
  else if a.hash < b.hash then 1
  else if bool_to_int a.preferred > bool_to_int b.preferred then (-1)
  else if bool_to_int a.preferred < bool_to_int b.preferred then 1
  else if a.length > b.length then (-1)
  else if a.length < b.length then 1
  else compare a b

let hash x =
  let res = ref 0 in
  for i = 0 to String.length x - 1
  do if x.[i] <> ' ' then res := (!res lsr 2) + (Char.code x.[i] lsl 24) done ;
  !res

let make_entry ~kind ~length ?(preferred= false) ?(name= "") ?(delta= Zero) uid =
  { uid; kind; length; preferred; hash= hash name; delta; }

module Utils = struct
  let length_of_variable_length n =
    let rec go r = function 0 -> r | n -> go (succ r) (n lsr 7) in
    go 1 (n lsr 7)

  let length_of_copy_code n =
    let rec go r n = if n = 0 then r else go (succ r) (n lsr 8) in
    if n = 0 then 1 else go 0 n

  let length ~source ~target hunks =
    length_of_variable_length source +
    length_of_variable_length target +
    List.fold_left (fun acc -> function
        | Duff.Insert (_, len) -> 1 + len + acc
        | Duff.Copy (off, len) ->
          1 + length_of_copy_code off +
          (if len = 0x10000 then 1 else length_of_copy_code len) +
          acc)
      0 hunks

  let has_only_inserts = List.for_all (function Duff.Insert _ -> true | _ -> false)
end

let _max_depth = 60

module W = struct
  type 'a t = 'a Weak.t

  let create () = Weak.create 1
  let create_with v =
    let t = Weak.create 1 in
    Weak.set t 0 (Some v) ; t
  let set t v = Weak.set t 0 (Some v)
  let get t = Weak.get t 0
end

type 'uid p = { index : Duff.index W.t
              ; entry : 'uid entry
              ; depth : int
              ; v : Dec.v W.t }

type 'uid patch = { hunks : Duff.hunk list
                  ; depth : int
                  ; source : 'uid
                  ; source_length : int }

type 'uid q = { mutable patch : 'uid patch option
              ; entry : 'uid entry
              ; v : Dec.v W.t }

let pp_patch target_length pp_uid ppf patch =
  Fmt.pf ppf "{ @[<hov>hunks= %d;@ \
                       depth= %d;@ \
                       source= %a;@ \
                       source_length= %d;@] }"
    (Utils.length ~source:patch.source_length ~target:target_length patch.hunks)
    patch.depth pp_uid patch.source patch.source_length

let pp_kind ppf = function
  | `A -> Fmt.string ppf "a"
  | `B -> Fmt.string ppf "b"
  | `C -> Fmt.string ppf "c"
  | `D -> Fmt.string ppf "d"

let pp_delta pp_uid ppf = function
  | Zero -> Fmt.string ppf "<none>"
  | From uid -> Fmt.pf ppf "@[<1>(From %a)@]" pp_uid uid

let pp_entry pp_uid ppf entry =
  Fmt.pf ppf "{ @[<hov>uid= %a;@ \
                       kind= %a;@ \
                       length= %d;@ \
                       preferred= %b;@ \
                       hash= %d;@ \
                       delta= @[<hov>%a@];@] }"
    pp_uid entry.uid
    pp_kind entry.kind
    entry.length entry.preferred entry.hash
    (pp_delta pp_uid) entry.delta

let pp_q pp_uid ppf q =
  Fmt.pf ppf "{ @[<hov>patch= @[<hov>%a@]; \
                       entry= @[<hov>%a@]; \
                       v= %s@] }"
    Fmt.(Dump.option (pp_patch q.entry.length pp_uid)) q.patch
    (pp_entry pp_uid) q.entry
    (if Weak.check q.v 0 then "#raw" else "NULL")

type ('uid, 's) load = 'uid -> (Dec.v, 's) io

let depth_of_source : 'uid p -> int = fun { depth; _ } -> depth

let depth_of_target : 'uid q -> int = fun { patch; _ } -> match patch with
  | None -> 1 | Some { depth; _ } -> depth

let target_to_source
  : 'uid q -> 'uid p
  = fun target ->
    { index= W.create ()
    ; entry= target.entry
    ; depth= depth_of_target target
    ; v= target.v (* XXX(dinosaure): dragoon here! *) }

let entry_to_target
  : type s. s scheduler -> load:('uid, s) load -> 'uid entry -> ('uid q, s) io
  = fun { bind; return; } ~load entry ->
    let ( >>= ) = bind in

    load entry.uid >>= fun v -> (match entry.delta with
        | From uid ->
          load uid >>= fun s ->
          let index = Duff.make (Bigstringaf.sub ~off:0 ~len:(Dec.len s) (Dec.raw s)) in
          let hunks = Duff.delta index (Bigstringaf.sub ~off:0 ~len:(Dec.len v) (Dec.raw v)) in
          return (Some { hunks; depth= Dec.depth v; source= uid; source_length= Dec.len s; })
        | Zero -> return None) >>= fun patch -> return { patch; entry; v= W.create_with v; }

let length_of_delta ~source ~target hunks = Utils.length ~source ~target hunks

exception Break
exception Next

let apply { bind; return; } ~load ~uid_ln ~(source:'uid p) ~(target:'uid q) =
  let ( >>= ) = bind in

  if source.entry.kind <> target.entry.kind
  then raise_notrace Break ;

  if depth_of_source source >= _max_depth
  then raise_notrace Next ;

  let max_length, ref_depth = match target.patch with
    | Some { hunks; source_length; depth; _ } ->
      length_of_delta ~source:source_length ~target:target.entry.length hunks, depth
    | None ->
      target.entry.length / 2 - uid_ln, 1 in

  let max_length = max_length * (_max_depth - depth_of_source source) / (_max_depth - ref_depth + 1) in

  if max_length == 0 then raise_notrace Next ;

  let diff =
    if source.entry.length < target.entry.length
    then target.entry.length - source.entry.length else 0 in

  if diff >= max_length then raise_notrace Next ;
  if target.entry.length < source.entry.length / 32 then raise_notrace Next ;
  (* if not (same_island target.entry.uid source.entry.uid) then raise_notrace Next ; *)

  let load_if weak uid = match W.get weak with
    | Some v -> return v
    | None ->
      load uid >>= fun v -> W.set weak v ; return v in
  let index_if weak v = match W.get weak with
    | Some index -> index
    | None ->
      let index = Duff.make (Bigstringaf.sub ~off:0 ~len:(Dec.len v) (Dec.raw v)) in
      W.set weak index ; index in

  load_if source.v source.entry.uid >>= fun source_v ->
  load_if target.v target.entry.uid >>= fun target_v ->
  index_if source.index source_v |> fun source_index ->

  let hunks = Duff.delta source_index (Bigstringaf.sub ~off:0 ~len:(Dec.len target_v) (Dec.raw target_v)) in

  target.patch <- Some { hunks
                        ; source= source.entry.uid
                        ; source_length= source.entry.length
                        ; depth= source.depth + 1; } ;
  return ()

module Delta (Scheduler : SCHEDULER) (IO : IO with type 'a t = 'a Scheduler.s) (Uid : UID) = struct
  module K = struct type t = Uid.t let hash = Hashtbl.hash let equal = Uid.equal end
  module V = struct type t = Uid.t p let weight ({ entry; _ } : Uid.t p) = entry.length end
  module Window = Lru.M.Make(K)(V)

  let ( >>= ) = IO.bind
  let return = IO.return

  let s =
    let open Scheduler in
    { bind= (fun x f -> inj (IO.bind (prj x) (fun x -> prj (f x))))
    ; return= (fun x -> inj (IO.return x)) }

  let same_island : 'uid -> 'uid -> bool = fun _ _ -> true
  (* XXX(dinosaure): TODO [delta-islands]! *)

  let delta ~load ~weight targets =
    let window = Window.create weight in

    let go target =
      let best : Window.k option ref = ref None in
      let f (key : Window.k) source =
        try ( apply s ~load ~uid_ln:Uid.length ~source ~target |> Scheduler.prj >>= fun () -> best := (Some key) ; return () )
        with Next -> return ()
           | Break as exn -> raise_notrace exn in
      let rec go = function
        | [] -> return ()
        | (k, v) :: r -> ( try f k v >>= fun () -> (go[@tailcall]) r with Break -> return () ) in
      go (Window.to_list window) >>= fun () ->
      ( match !best with
        | Some best ->
          let[@warning "-8"] Some source = Window.find best window in
          if source.depth < _max_depth
          then Window.promote best window
        | None -> () ) ; return () in
    let rec map i =
      if i < Array.length targets
      then
        ( go targets.(i) >>= fun () ->
          Window.add targets.(i).entry.uid (target_to_source targets.(i)) window
        ; (map[@tailcall]) (succ i) )
      else return () in
    map 0

  type m = { mutable v : int; m : IO.Mutex.t }

  let dispatcher
    : type uid. load:(uid, Scheduler.t) load -> mutex:m -> entries:uid entry array -> targets:uid q array -> unit IO.t
    = fun ~load ~mutex ~entries ~targets ->
      let rec go () =
        IO.Mutex.lock mutex.m >>= fun () ->
        let v = mutex.v in mutex.v <- mutex.v + 1 ;
        if v >= Array.length entries
        then ( IO.Mutex.unlock mutex.m ; IO.return () )
        else ( IO.Mutex.unlock mutex.m
             ; entry_to_target s ~load entries.(v) |> Scheduler.prj >>= fun target ->
               targets.(v) <- target
             ; go () ) in
      go ()

  let dummy : Uid.t q =
    { patch= None
    ; entry= { uid= Uid.null
             ; kind= `A
             ; length= 0
             ; preferred= false
             ; hash= 0
             ; delta= Zero }
    ; v= W.create () }

  let delta ~threads ~weight entries =
    let mutex = { v= 0; m= IO.Mutex.create () } in
    let targets = Array.make (Array.length entries) dummy in
    IO.nfork_map
      ~f:(fun load -> dispatcher ~load ~mutex ~entries ~targets)
      threads
    >>= fun futures -> IO.all_unit (List.map IO.Future.wait futures)
    >>= fun () -> delta ~load:(List.hd threads) ~weight targets
    >>= fun () -> return targets
end

module N : sig
  type encoder

  type tmp =
    { i : Bigstringaf.t
    ; q : Dd.B.t
    ; w : Dd.window }

  val encoder : 's scheduler -> tmp:tmp -> load:('uid, 's) load -> 'uid q -> (encoder, 's) io
  val encode : o:Bigstringaf.t -> encoder -> [ `Flush of (encoder * int) | `End ]
  val dst : encoder -> Bigstringaf.t -> int -> int -> encoder
end = struct
  type tmp =
    { i : Bigstringaf.t (* to store [h] output. *)
    ; q : Dd.B.t
    ; w : Dd.window }

  type encoder =
    | H of Zh.N.encoder
    | Z of Zz.N.encoder

  let rec encode_zlib ~o encoder = match Zz.N.encode encoder with
    | `Await encoder ->
      encode_zlib ~o (Zz.N.src encoder Bigstringaf.empty 0 0)
    | `Flush encoder ->
      let len = Bigstringaf.length o - Zz.N.dst_rem encoder in
      `Flush (encoder, len)
    | `End encoder ->
      let len = Bigstringaf.length o - Zz.N.dst_rem encoder in
      if len > 0
      then `Flush (encoder, len)
      else `End

  let encode_hunk ~o encoder = match Zh.N.encode encoder with
    | `Flush encoder ->
      let len = Bigstringaf.length o - Zh.N.dst_rem encoder in
      `Flush (encoder, len)
    | `End -> `End

  let encode ~o = function
    | Z encoder -> ( match encode_zlib ~o encoder with `Flush (encoder, len) -> `Flush (Z encoder, len) | `End -> `End )
    | H encoder -> ( match encode_hunk ~o encoder with `Flush (encoder, len) -> `Flush (H encoder, len) | `End -> `End )

  let dst encoder s j l = match encoder with
    | Z encoder -> let encoder = Zz.N.dst encoder s j l in Z encoder
    | H encoder -> let encoder = Zh.N.dst encoder s j l in H encoder

  let compress { bind; return; } ~tmp ~load target =
    let ( >>= ) = bind in

    let load_if weak uid = match W.get weak with
      | Some v -> return v
      | None ->
        load uid >>= fun v -> W.set weak v ; return v in

    load_if target.v target.entry.uid >>= fun v ->
    let encoder = Zz.N.encoder `Manual `Manual ~q:tmp.q ~w:tmp.w ~level:4 in
    let encoder = Zz.N.src encoder (Dec.raw v) 0 (Dec.len v) in
    let encoder = Zz.N.dst encoder tmp.i 0 (Bigstringaf.length tmp.i) in

    return (Z encoder)

  let encoder
    : type s. s scheduler -> tmp:tmp -> load:('uid, s) load -> 'uid q -> (encoder, s) io
    = fun ({ bind; return; } as s) ~tmp ~load target ->
      let ( >>= ) = bind in

      let load_if weak uid = match W.get weak with
        | Some v -> return v
        | None ->
          load uid >>= fun v -> W.set weak v ; return v in

      match target.patch with
      | Some { hunks; source_length; _ } ->
        load_if target.v target.entry.uid >>= fun v ->
        let raw = Bigstringaf.sub ~off:0 ~len:(Dec.len v) (Dec.raw v) in
        let encoder = Zh.N.encoder ~i:tmp.i ~q:tmp.q ~w:tmp.w ~source:source_length raw `Manual hunks in
        return (H encoder)
      | None -> compress s ~tmp ~load target
end
