package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
)

// list prints every finalized clip the server holds as an aligned table. An empty store prints
// just the header, so the column names always appear and a script can tell "no clips" from "request
// failed" by the exit code rather than by parsing emptiness. The created and expiry columns are
// rendered as spans from one clock read taken before the loop, so every row in a single listing
// measures against the same present and two rows created together read alike.
func list(ctx context.Context, c *client.Client, std IO) error {
	clips, err := c.List(ctx)
	if err != nil {
		return err
	}
	now := std.now()
	tw := tabwriter.NewWriter(std.Out, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tKIND\tSIZE\tCREATED\tEXPIRES\tFLAGS")
	for _, cl := range clips {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			safeField(cl.Name), safeField(string(cl.Meta.Kind)), humanSize(cl.Size),
			createdText(now, cl.CreatedAt), expiresText(now, cl.ExpiresAt), flagText(cl))
	}
	return buffErr(tw.Flush())
}

// stat prints one clip's metadata as an aligned key-value block. It reports the fields a metadata
// probe carries — the generation, kind, optional filename, size, finalized and consume-once
// flags, and the expiry — and not the created or finalized instants, which the probe does not
// return and which would only ever print as a misleading zero time. The size and expiry get that
// same treatment when the clip is still live: both are settled only at finalize, so a live probe
// carries them as zero, and a dash says "not yet known" where a literal 0B or never would assert a
// definite, wrong value.
func stat(ctx context.Context, c *client.Client, inv invocation, std IO) error {
	cl, err := c.Stat(ctx, inv.slot)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(std.Out, 0, 2, 1, ' ', 0)
	fmt.Fprintf(tw, "name:\t%s\n", safeField(cl.Name))
	fmt.Fprintf(tw, "generation:\t%s\n", safeField(cl.Generation))
	fmt.Fprintf(tw, "kind:\t%s\n", safeField(string(cl.Meta.Kind)))
	if cl.Meta.Filename != "" {
		fmt.Fprintf(tw, "filename:\t%s\n", safeField(cl.Meta.Filename))
	}
	// Shown only when set, like the filename: it is file-clip identity absent from every bytes clip,
	// so printing executable:false on the common clip would be noise rather than information.
	if cl.Meta.Executable {
		fmt.Fprintf(tw, "executable:\t%t\n", cl.Meta.Executable)
	}
	// A live first-write is reachable here — resolveRead follows it — but the server withholds
	// the size and expiry of a generation still being written, so cl carries them as zero. Only a
	// finalized clip has real values to show; for a live one a dash is the honest rendering, which the
	// finalized:false line just below confirms.
	size, expires := "-", "-"
	if cl.Finalized {
		size, expires = humanSize(cl.Size), expiresText(std.now(), cl.ExpiresAt)
	}
	fmt.Fprintf(tw, "size:\t%s\n", size)
	fmt.Fprintf(tw, "finalized:\t%t\n", cl.Finalized)
	fmt.Fprintf(tw, "consume:\t%t\n", cl.ConsumeOnce)
	fmt.Fprintf(tw, "expires:\t%s\n", expires)
	return buffErr(tw.Flush())
}

// humanSize renders a byte count in binary units, so a listing reads at a glance rather than in raw
// bytes. Sub-kibibyte sizes stay exact in bytes; larger ones round to one decimal of the largest
// unit that fits.
func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// humanDuration renders a non-negative span compactly in the largest whole unit that fits — the
// duration mirror of humanSize: seconds below a minute, minutes below an hour, hours above, and a
// flat 0s for anything under a second. It truncates to that unit rather than rounding, which keeps
// a remaining span honest by never claiming more time than is left — a clip with 59m to run reads
// "in 59m", not the "in 1h" rounding would inflate it to. The vocabulary stops at hours on purpose:
// a TTL is a Go duration, which has no day unit, so the listing speaks back exactly the units
// a user can type with --ttl, and a multi-day kept clip simply reads in large hours rather than
// forcing in a unit the input side would reject.
func humanDuration(d time.Duration) string {
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	case d >= time.Minute:
		return fmt.Sprintf("%dm", int64(d/time.Minute))
	case d >= time.Second:
		return fmt.Sprintf("%ds", int64(d/time.Second))
	default:
		return "0s"
	}
}

// createdText renders a clip's creation instant as how long ago it was — the question a listing of
// ephemeral clips actually asks, "how fresh is this", not a wall-clock instant a reader would have
// to lift out of the server's zone and subtract from the present by hand. A span under a second,
// which includes the slightly negative one a client clock running ahead of the server's produces,
// reads as "just now"; a zero instant — which a finalized clip never carries, but a defensive
// caller might — stays the dash an empty cell uses everywhere else.
func createdText(now, t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	if d := now.Sub(t); d >= time.Second {
		return humanDuration(d) + " ago"
	}
	return "just now"
}

// expiresText renders an expiry instant as the time left until it — the half of the listing a TTL
// exists to govern. A zero instant is the kept-forever sentinel and must read as "never", never
// as a date; an instant already past reads as "expired", which is honest even though the bytes
// linger readable until the reaper's next sweep removes them, because the deadline itself has
// passed. Rendering the span rather than the instant is also what surfaces a sub-minute TTL at
// all: it shows "in 9s" where a wall-clock "15:04" would round both creation and expiry into the
// same minute.
func expiresText(now, t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	if d := t.Sub(now); d > 0 {
		return "in " + humanDuration(d)
	}
	return "expired"
}

// flagText names the per-clip flags worth showing in the listing — consume-once and the executable
// bit — joined so a clip carrying both shows both, and rendered as a dash when it has neither so
// the column is never blank.
func flagText(cl clip.Clip) string {
	var flags []string
	if cl.ConsumeOnce {
		flags = append(flags, "consume")
	}
	if cl.Meta.Executable {
		flags = append(flags, "exec")
	}
	if len(flags) == 0 {
		return "-"
	}
	return strings.Join(flags, ",")
}

// safeField renders a string inert for a terminal before a metadata probe prints it. A probe (buff
// -l, buff -s) is a "what's here" question, so a field that originates with the server — a foreign
// or hostile peer may be on the other end — must never drive the terminal: a C0/C1 control or DEL
// (the ESC and CSI introducers among them), or an invalid UTF-8 byte that is itself a raw control,
// would otherwise move the cursor, recolour the screen, or break column alignment with an injected
// tab or newline. A field carrying any such byte is shown quoted, every escape made visible; a
// clean field — printable runes, non-ASCII names included — is returned untouched, so the ordinary
// listing reads exactly as before. Applied uniformly to every rendered string so the rule survives
// a refactor that changes a field's source, not just today's server-controlled ones. content-
// show (buff @clip) streams raw bytes by its nature and keeps that behaviour; this guards only the
// probe's metadata. Out of scope by the same line: BiDi and confusable runes, a deeper concern that
// also reaches content — the boundary here is the control bytes that act on a terminal.
func safeField(s string) string {
	for _, r := range s {
		if r == utf8.RuneError || unicode.IsControl(r) {
			return strconv.Quote(s)
		}
	}
	return s
}
