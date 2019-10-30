open Prelude
open Core

let z = De.bigstring_create De.io_buffer_size
let allocate bits = De.make_window ~bits

module Cache : sig
  type t
  type raw = Carton.Dec.raw

  val create : capacity:int -> t
  val malloc : weight:Carton.Dec.weight -> t -> raw
  val free : raw -> t -> unit
end = struct
  type t =
    { contents : Carton.Dec.raw Weak.t
    ; mutex : Mutex.t
    ; mask : int
    ; mutable idx : int; }
  and raw = Carton.Dec.raw

  let (//) x y =
    if y < 0 then raise Division_by_zero
    else ( if x > 0 then 1 + ((x - 1) / y) else 0)

  let pagesize = 4096
  let round_of_pagesize x = x // pagesize * pagesize

  let with_lock ~f mutex =
    Mutex.lock mutex ; f () ; Mutex.unlock mutex

  let[@inline always] is_power_of_two v = v <> 0 && v land (lnot v + 1) = v

  let create ~capacity =
    if not (is_power_of_two capacity) then Fmt.invalid_arg "Cache.create" ;

    let mutex = Mutex.create () in
    { contents= Weak.create capacity
    ; mutex
    ; mask = capacity - 1
    ; idx= 0 }

  let malloc ~(weight:Carton.Dec.weight) t =
    let weight = Carton.Dec.weight_of_int_exn (round_of_pagesize (weight :> int)) in
    let res = ref None in
    let exception Found in

    let f () =
      try
        for i = 0 to Weak.length t.contents - 1 do
          match Weak.get t.contents i with
          | Some raw as v ->
            let weight' = Carton.Dec.weight_of_raw raw in
            if weight <= weight' then ( res := v ; Weak.set t.contents i None ; raise_notrace Found )
          | None -> ()
        done ;
        let raw = Carton.Dec.make_raw ~weight in res := Some raw
      with Found -> () in
    with_lock ~f t.mutex ; let[@warning "-8"] Some v = !res in v

  let free raw t =
    let f () =
      Weak.set t.contents (t.idx land t.mask) (Some raw) ; t.idx <- t.idx + 1 in
    with_lock ~f t.mutex
end

(* Verbose mode *)

let mutex = Mutex.create ()
let verbose = ref false

let record uid fpath =
  if !verbose
  then ( Mutex.lock mutex
       ; Fmt.pr "Store %a to %a.\n%!" Uid.pp uid Fpath.pp fpath
       ; Mutex.unlock mutex )

let pp_kind ppf = function
  | `A -> Fmt.pf ppf "a"
  | `B -> Fmt.pf ppf "b"
  | `C -> Fmt.pf ppf "c"
  | `D -> Fmt.pf ppf "d"

let pp_uid ppf x = Uid.pp ppf x

exception Invalid_path of string

let path fformat v =
  let pp = Fformat.format ~pp_kind ~pp_uid fformat in
  match Fpath.of_string (Fmt.strf "%a" pp v) with
  | Ok v -> v
  | Error _ -> raise (Invalid_path (Fmt.strf "%a" pp v))

exception Not_found of Uid.t

module Ip = Carton.Dec.Ip(Us)(IO)(Uid)

type nonsense = |

let unpack_with_idx ~digest:_ threads fformat lru idx fpath =
  let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mx = let st = Unix.LargeFile.fstat fd in st.Unix.LargeFile.st_size in
  let pack =
    Carton.Dec.make { fd; mx; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string
      (fun uid -> match Carton.Dec.Idx.find idx uid with
         | Some (_, offset) -> offset
         | None -> raise (Not_found uid)) in

  let f pack ~uid:uid ~offset:_ ~crc:_ =
    let fiber () =
      let ( >>= ) = unix.Carton.bind in
      let return = unix.Carton.return in

      Carton.Dec.weight_of_uid unix ~map:unix_map pack ~weight:Carton.Dec.null uid >>= fun weight ->
      let raw = Cache.malloc ~weight lru in
      assert (Carton.Dec.weight_of_raw raw >= weight) ;
      Carton.Dec.of_uid unix ~map:unix_map pack raw uid >>= fun v ->
      let kind = Carton.Dec.kind v in
      let payload = Carton.Dec.raw v in
      let len = Carton.Dec.len v in
      let fpath = path fformat (kind, uid) in

      let fiber : ((unit, nonsense) result, Rresult.R.msg) result =
        let open Rresult.R in Bos.OS.Dir.create ~path:true (Fpath.parent fpath) >>= fun _ ->
        Bos.OS.File.with_oc fpath (fun oc raw -> output_string oc raw ; Ok ()) (Bigstringaf.substring payload ~off:0 ~len) in

      match fiber with
      | Ok (Ok ()) -> record uid fpath ; Cache.free raw lru ; return ()
      | Ok (Error _) -> .
      | Error _ as err -> Rresult.R.failwith_error_msg err in
    Us.prj (fiber ())
  in

  let threads =
    let t0 = pack in

    List.init threads (fun _ ->
        let z = Bigstringaf.copy z ~off:0 ~len:(Bigstringaf.length z) in
        let w = allocate 15 in

        Carton.Dec.with_w (Carton.Dec.W.make { fd; mx; }) t0
        |> Carton.Dec.with_z z
        |> Carton.Dec.with_allocate ~allocate:(fun _ -> w)) in
  Ip.iter ~threads ~f idx ; Ok ()

module V = Carton.Dec.Verify(Uid)(Us)(IO)

let unpack_without_idx ~digest threads fformat fpath =
  let open Rresult.R in
  Verify.first_pass ~digest fpath >>= fun (oracle, matrix, _, _, _) ->
  let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mx = let st = Unix.LargeFile.fstat fd in st.Unix.LargeFile.st_size in
  let index uid = raise (Not_found uid) in
  let t = Carton.Dec.make { fd; mx; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string index in

  let digest ~kind ?(off= 0) ?len buf =
    let len = match len with Some len -> len | None -> Bigstringaf.length buf - off in
    let uid = digest ~kind ~off ~len buf in
    let fpath = path fformat (kind, uid) in

    let fiber : ((unit, nonsense) result, Rresult.R.msg) result =
      let open Rresult.R in Bos.OS.Dir.create ~path:true (Fpath.parent fpath) >>= fun _ ->
      Bos.OS.File.with_oc fpath (fun oc raw -> output_string oc raw ; Ok ()) (Bigstringaf.substring buf ~off ~len) in

    match fiber with
    | Ok (Ok ()) -> record uid fpath ; uid
    | Ok (Error _) -> .
    | Error _ as err -> Rresult.R.failwith_error_msg err in
  let oracle = { oracle with Carton.Dec.digest= digest } in

  V.verify ~threads ~map:unix_map ~oracle t ~matrix ; Unix.close fd ; Ok ()

let unpack ~digest threads v fformat fpath =
  verbose := v ;

  let idx = Fpath.set_ext "idx" fpath in

  if Sys.file_exists (Fpath.to_string idx)
  then
    let stat = Unix.stat (Fpath.to_string idx) in
    let fd = Unix.openfile (Fpath.to_string idx) Unix.[ O_RDONLY ] 0o644 in
    let mp = Mmap.V1.map_file fd ~pos:0L Bigarray.char Bigarray.c_layout false [| stat.Unix.st_size |] in
    let mp = Bigarray.array1_of_genarray mp in
    let idx = Carton.Dec.Idx.make mp ~uid_ln:Uid.length ~uid_rw:Uid.to_raw_string ~uid_wr:Uid.of_raw_string in
    let lru = Cache.create ~capacity:0x10000 in

    let res = unpack_with_idx ~digest threads fformat lru idx fpath in
    Unix.close fd ; res
  else unpack_without_idx ~digest threads fformat fpath

open Cmdliner

let existing_fpath ~ext =
  let parser x = match Fpath.of_string x with
    | Ok v when Sys.file_exists x ->
      if Fpath.has_ext ext v then Ok v else Rresult.R.error_msgf "%a has a bad extension" Fpath.pp v
    | Ok v -> Rresult.R.error_msgf "%a does not exist" Fpath.pp v
    | Error _ as err -> err in
  let pp = Fpath.pp in
  Arg.conv (parser, pp)

let fformat =
  let parser = Fformat.parse in
  let pp = Fformat.pp in
  Arg.conv (parser, pp)

let pack =
  let doc = "The packed archive." in
  Arg.(required & pos 0 ~rev:true (some (existing_fpath ~ext:"pack")) None & info [] ~docv:"<pack>.pack" ~doc)

let number ~default =
  let parser x =
    try let x = int_of_string x in if x <= 0 then Ok default else Ok x
    with _ -> Rresult.R.error_msgf "Invalid number: %S" x in
  let pp = Fmt.int in
  Arg.conv (parser, pp)

let threads =
  let doc = "Specifies the number of threads to spawn when resolving deltas. \
             This is meant to reduce packing time on multiprocessor machines. \
             The required amount of memory for the delta search window is however \
             multiplied by the number of threads." in
    Arg.(value & opt (number ~default:cpu) cpu & info [ "threads" ] ~doc ~docv:"<n>")

let fformat =
  let doc = "Format of path to store objects." in
  Arg.(required & opt (some fformat) None & info [ "f"; "format" ] ~doc ~docv:"<format>")

let verbose =
  let doc = "Say where objects are stored." in
  Arg.(value & flag & info [ "v"; "verbose" ] ~doc)

let cmd ~digest =
  let doc = "Unpack objects from a packed archive." in
  let exits = Term.default_exits in
  let man =
    [ `S Manpage.s_description
    ; `P "Unpack objects from a packed archive." ] in
  Term.(const (unpack ~digest) $ threads $ verbose $ fformat $ pack),
  Term.info "unpack" ~doc ~exits ~man
