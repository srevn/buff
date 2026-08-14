# buff completions for fish.
#
# Two things about the grammar fish cannot infer on its own, and this file exists to teach it:
# @name is a slot while a bare word is a path — so slots and files are offered side by side, and the
# '@' the user has typed is what picks between them — and the options come in per-mode families, so
# an option is offered only where buff would accept it rather than everywhere and rejected on
# submit. Where the two disagree the parser is right and this file is the bug.
#
# Slot names are read out of `buff -l`. That table is a human render, so the coupling is deliberate
# and bounded to the fields that cannot move: every column this reads is structurally free of
# whitespace — a slot name may not contain any, a kind is a single word, units.Size renders "1.0KiB"
# unspaced, and the flag cell comma-joins — so they are addressed as the first three fields and the
# last, never by counting across the middle. CREATED and EXPIRES are the two that are written as
# human spans ("just now", "in 3h") and so vary in width; they sit between, and are skipped for
# exactly that reason.
#
# What the listing cannot offer is a name that is not there yet: only finalized clips are listed, so
# a slot mid-write, or one never written, has no candidate — which is precisely what --wait and
# --follow-next address, the one gap here that no amount of querying closes.
#
# Every completion below is a metadata call — the listing or a HEAD — never a read, so completing a
# name can never spend a consume-once clip's one delivery. Nor can a hostile name forge a candidate
# or drive the terminal, and that matters because the server owns the namespace and this client
# validated none of it: buff renders any listing field carrying a control byte quoted and inert, so
# a name containing a newline collapses onto its own single row rather than becoming a second slot,
# and no escape introducer survives into the terminal fish prints these candidates to. Such a name
# can still split at a field boundary into a truncated candidate, which is harmless in the one way
# that counts — it is not a name buff will accept if it is ever submitted.
#
# The listing is fetched on each keypress rather than cached. A local server answers in a few
# milliseconds and a clipboard's namespace turns over constantly, so a cache would trade away the
# freshness that matters — the slot written a second ago is exactly the one being completed — for
# latency that is already imperceptible. The cost of that choice is a distant or unreachable server:
# buff sets no whole-request timeout, deliberately, since one would cut off a legitimate live
# follow, so a completion against a dead server waits on the connection attempt and is interrupted
# with ctrl-C like any other foreground command.

# --- reading the command line ----------------------------------------------

# The --server override already on the line, as the arguments to pass through. Without this a
# completion would query whichever server the environment points at while the command being typed
# targets another, and offer names that do not exist there. The scan runs to the end of the line and
# keeps the last value rather than returning at the first, because that is how buff's own parse
# resolves a repeated flag — a completion that answered from an earlier --server than the one the
# command will use is the same wrong-server answer in a subtler form.
function __buff_server_args
    set -l toks (commandline -opc)
    set -l url
    for i in (seq (count $toks))
        if test "$toks[$i]" = --server
            set -l next (math $i + 1)
            test $next -le (count $toks); and set url $toks[$next]
        else if string match -q -- '--server=*' $toks[$i]
            set url (string split -m1 = -- $toks[$i])[2]
        end
    end
    if set -q url[1]
        echo --server
        echo $url
    end
end

# The slot already complete on the line, without its sigil. The token under the cursor is not among
# these, so a half-typed "@wor" does not count as a slot already given.
function __buff_slot_on_line
    for t in (commandline -opc)
        if string match -qr '^@.' -- $t
            string sub -s 2 -- $t
            return 0
        end
    end
    return 1
end

function __buff_has_slot
    __buff_slot_on_line >/dev/null
end

# serve is the single reserved first token, so it is a subcommand only in first position — a file
# named serve is copied as ./serve and must not put the completions into server mode.
function __buff_serve
    set -l toks (commandline -opc)
    set -q toks[2]; and test "$toks[2]" = serve
end

# Nothing typed yet but the command itself, which is the only position serve can be offered in. The
# generic "no subcommand seen yet" test is wrong here: it looks past leading flags, and would keep
# offering serve after "buff -l", where the word would be read as a path and rejected as one.
function __buff_first_token
    test (count (commandline -opc)) -eq 1
end

function __buff_client
    not __buff_serve
end

# The management actions, which share a shape: none takes a path, and two of them take no slot
# either. Mode is otherwise settled by the stream types, which a completion cannot see, so only an
# explicit force or a management flag is treated as having settled it.
function __buff_manage
    __fish_contains_opt -s l list; or __fish_contains_opt -s d delete
    or __fish_contains_opt -s s stat; or __fish_contains_opt version
end

# Listing and version address nothing at all — delete and stat each address one slot, and copy and
# paste default to @default — so a slot candidate beside -l is one the parser rejects on submit.
function __buff_addresses_slot
    not __fish_contains_opt -s l list; and not __fish_contains_opt version
end

function __buff_forced_copy
    __fish_contains_opt -s c copy
end

function __buff_forced_paste
    __fish_contains_opt -s p paste
end

# A forced mode is a statement that this line copies or pastes, which is precisely what a management
# action is not — buff rejects the pair — so a forced line withdraws the actions, the mirror of a
# management line withdrawing the forces.
function __buff_forced_mode
    __buff_forced_copy; or __buff_forced_paste
end

# --- what buff can be asked ------------------------------------------------

# The namespace, re-sigiled, with each clip's kind, size, and flags as the description — the columns
# worth seeing at the moment of choosing a slot, since they say whether tab-completing this one and
# pasting it will print bytes, write a file, or spend a one-shot delivery. NR skips the header, which
# is also what makes an empty store offer nothing rather than a phantom "NAME" slot, and the field
# count is asserted rather than assumed so a line that is not a clip row can never become a
# candidate. A server that is unreachable, slow, or refusing simply yields none.
function __buff_slots
    command buff (__buff_server_args) -l 2>/dev/null | awk 'NR > 1 && NF >= 6 {
        desc = $2 " " $3
        if ($NF != "-") desc = desc " " $NF
        printf "@%s\t%s\n", $1, desc
    }'
end

# The current generation of the slot on the line, plus the any-clip wildcard. This is the one
# --if-match value a person cannot reasonably type, and a HEAD answers it without touching the bytes.
function __buff_generation
    set -l slot (__buff_slot_on_line); or return
    command buff (__buff_server_args) -s @$slot 2>/dev/null |
        string match -rg '^generation:\s+(\S+)'
    echo '*'
end

# --- the server -------------------------------------------------------------

complete -c buff -n __buff_first_token -a serve -d 'run the content-relay server'

complete -c buff -n __buff_serve -o data-dir -x -a '(__fish_complete_directories)' -d 'storage root, required (BUFF_DATA_DIR)'
complete -c buff -n __buff_serve -o addr -x -d 'listen address (BUFF_ADDR)'
complete -c buff -n __buff_serve -o max-clip -x -a '0 64MiB 256MiB 1GiB 4GiB' -d 'per-clip byte cap, 0=unlimited'
complete -c buff -n __buff_serve -o max-total -x -a '0 1GiB 10GiB 100GiB' -d 'total byte cap, 0=unlimited'
complete -c buff -n __buff_serve -o max-clips -x -a '0 100 1000 10000' -d 'clip-count cap, 0=unlimited'
complete -c buff -n __buff_serve -o ttl -x -a '0 1h 24h 7d 2w' -d 'default retention, 0=none'
complete -c buff -n __buff_serve -o reap-interval -x -a '0 10s 60s 5m' -d 'reaper tick, 0=off'
complete -c buff -n __buff_serve -o upload-idle -x -a '10s 30s 5m' -d 'per-request idle deadline'
complete -c buff -n __buff_serve -o upload-max -x -a '0 1h 6h' -d 'max upload duration, 0=off'
complete -c buff -n __buff_serve -o wait-max -x -a '0 30s 5m 1h' -d 'max wait-GET park, 0=off'
complete -c buff -n __buff_serve -o fsync -x -a 'on off' -d 'durable commit'
complete -c buff -n __buff_serve -o checksum -x -a 'on off' -d 'store and verify CRC32C'

# --- slots and paths --------------------------------------------------------

# Slots are offered alongside fish's file completion, never instead of it: that is the @-is-a-slot,
# bare-word-is-a-path rule carried into the shell. A line that already names a slot gets no more,
# because buff addresses exactly one.
complete -c buff -n '__buff_client; and __buff_addresses_slot; and not __buff_has_slot' -a '(__buff_slots)' -d slot

# A management action addresses a slot or nothing, never a path, so file completion is withdrawn
# once one is on the line — offering a filename there only invites the mistake of writing a slot
# without its '@'.
complete -c buff -n '__buff_client; and __buff_manage' -f

# --- client options ---------------------------------------------------------

complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_paste' -s c -l copy -d 'force copy'
complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_copy' -s p -l paste -d 'force paste'

complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_copy' -s o -l output -r -F -d 'paste: destination (- for stdout)'
complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_copy' -l wait -d 'paste: block until the slot is written'
complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_copy' -l follow-next -d 'paste: skip the current value, follow the next write'

complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_paste' -l ttl -x -a '0 5m 30m 1h 6h 24h 7d 2w' -d 'copy: retention'
complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_paste' -l keep -d 'copy: never expire'
complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_paste' -l consume -d 'copy: deliver once, then destroy'
complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_paste' -l if-match -x -a '(__buff_generation)' -d 'copy: replace only if the generation matches'

# One action per line, so each withdraws the rest once chosen — and a forced mode withdraws them all.
complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_mode' -s l -l list -d 'list finalized clips'
complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_mode' -s d -l delete -d 'delete a clip'
complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_mode' -s s -l stat -d "show a clip's metadata"
complete -c buff -n '__buff_client; and not __buff_manage; and not __buff_forced_mode' -l version -d 'print the client version'

complete -c buff -n __buff_client -l server -x -d 'override BUFF_URL for this invocation'
complete -c buff -s h -l help -d 'print usage'
