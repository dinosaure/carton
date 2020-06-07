open Lwt.Infix

let failwithf fmt = Fmt.kstrf Lwt.fail_with fmt
let () = Lwt_preemptive.simple_init ()

let digest v =
  let kind = match Carton.Dec.kind v with
    | `A -> "commit" | `B -> "tree" | `C -> "blob" | `D -> "tag" in
  let hdr = Fmt.strf "%s %d\000" kind (Carton.Dec.len v) in
  let ctx = Digestif.SHA1.empty in
  let ctx = Digestif.SHA1.feed_string ctx hdr in
  let ctx = Digestif.SHA1.feed_bigstring ctx (Carton.Dec.raw v) ~off:0 ~len:(Carton.Dec.len v) in
  Digestif.SHA1.get ctx

module Uid = struct
  include Digestif.SHA1

  let null = digest_string ""
  let length = digest_size
  let compare = unsafe_compare
  let feed = feed_bigstring
end

module Pack = Carton_git_unix.Make(Uid)

let scan root =
  let root = Fpath.(root / "objects" / "pack") in
  Pack.make root ~idx:(Fpath.set_ext "idx") >>= fun t ->
  let fds = Pack.fds t in
  let pools =
    let fold fd =
      fd, Lwt_pool.create 4 @@ fun () ->
      let z = Bigstringaf.create De.io_buffer_size in
      let w = De.make_window ~bits:15 in
      let allocate _ = w in
      let w = Carton.Dec.W.make fd in
      Lwt.return { Carton_git.z; Carton_git.allocate; Carton_git.w } in
    List.map fold fds in
  let uids = Pack.list root t in
  let resources fd =
    Lwt_pool.use (List.assoc fd pools) in
  let get uid =
    Pack.get root ~resources t uid >>= function
    | Error (`Not_found uid) ->
      failwithf "%a not found" Uid.pp uid
    | Error (`Msg err) ->
      failwithf "%a: %s" Uid.pp uid err
    | Ok v ->
      let uid' = digest v in
      if Digestif.SHA1.equal uid uid'
      then ( Fmt.epr "%a: ok.\n%!" Uid.pp uid ; Lwt.return_unit )
      else failwithf "Error with %a (wrong uid)" Uid.pp uid in
  Lwt_list.iter_p get uids

let scan root = Lwt_main.run (scan root)

let () = match Sys.argv with
  | [| _; root |] ->
    let root = Fpath.v root in
    scan root
  | _ -> Fmt.epr "%s <.git>" Sys.argv.(0)
