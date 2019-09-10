let digest ~kind ?(off= 0) ?len buf =
  let len = match len with
    | Some len -> len
    | None -> Bigstringaf.length buf - off in
  let ctx = Digestif.SHA1.empty in

  let ctx = match kind with
    | `A -> Digestif.SHA1.feed_string ctx (Fmt.strf "commit %d\000" len)
    | `B -> Digestif.SHA1.feed_string ctx (Fmt.strf "tree %d\000" len)
    | `C -> Digestif.SHA1.feed_string ctx (Fmt.strf "blob %d\000" len)
    | `D -> Digestif.SHA1.feed_string ctx (Fmt.strf "tag %d\000" len) in
  let ctx = Digestif.SHA1.feed_bigstring ctx ~off ~len buf in
  Digestif.SHA1.get ctx

open Cmdliner

let cmds = [ Verify.cmd ~digest
           ; Get.cmd ~digest
           ; Stat.cmd ~digest
           ; Index.cmd ~digest
           ; Unpack.cmd ~digest
           ; Pack.cmd ~digest ]
let main = `Help (`Pager, None)

let cmd =
  let doc = "Carton" in
  let exits = Term.default_exits in
  let man =
    [ `S Manpage.s_description
    ; `P "Carton tool" ] in
  Term.(ret (const main)),
  Term.info "carton" ~doc ~exits ~man

let pp_error ppf = function
  | `Msg err -> Fmt.string ppf err
  | `Malformed err -> Fmt.string ppf err
  | `Invalid_pack -> Fmt.string ppf "Invalid packed archive"

let exit = function
    | `Ok (Ok ()) -> ()
    | `Ok (Error err) -> Fmt.epr "%a\n%!" pp_error err
    | v -> Term.exit v

let () = exit (Term.eval_choice cmd cmds)
