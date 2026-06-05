package cli

import (
	"context"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
)

// list prints every finalized clip the server holds as an aligned table. An empty store
// prints just the header, so the column names always appear and a script can tell "no clips"
// from "request failed" by the exit code rather than by parsing emptiness.
func list(ctx context.Context, c *client.Client, std IO) error {
	clips, err := c.List(ctx)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(std.Out, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tKIND\tSIZE\tCREATED\tEXPIRES\tFLAGS")
	for _, cl := range clips {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			cl.Name, cl.Meta.Kind, humanSize(cl.Size),
			shortTime(cl.CreatedAt), expiry(cl.ExpiresAt), flagText(cl))
	}
	return buffErr(tw.Flush())
}

// stat prints one clip's metadata as an aligned key-value block. It reports the fields a
// metadata probe carries — the generation, kind, optional filename, size, finalized and
// consume-once flags, and the expiry — and not the created or finalized instants, which the
// probe does not return and which would only ever print as a misleading zero time.
func stat(ctx context.Context, c *client.Client, inv invocation, std IO) error {
	cl, err := c.Stat(ctx, inv.slot)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(std.Out, 0, 2, 1, ' ', 0)
	fmt.Fprintf(tw, "name:\t%s\n", cl.Name)
	fmt.Fprintf(tw, "generation:\t%s\n", cl.Generation)
	fmt.Fprintf(tw, "kind:\t%s\n", cl.Meta.Kind)
	if cl.Meta.Filename != "" {
		fmt.Fprintf(tw, "filename:\t%s\n", cl.Meta.Filename)
	}
	// Shown only when set, like the filename: it is file-clip identity absent from every text clip,
	// so printing executable:false on the common clip would be noise rather than information.
	if cl.Meta.Executable {
		fmt.Fprintf(tw, "executable:\t%t\n", cl.Meta.Executable)
	}
	fmt.Fprintf(tw, "size:\t%s\n", humanSize(cl.Size))
	fmt.Fprintf(tw, "finalized:\t%t\n", cl.Finalized)
	fmt.Fprintf(tw, "consume:\t%t\n", cl.ConsumeOnce)
	fmt.Fprintf(tw, "expires:\t%s\n", expiry(cl.ExpiresAt))
	return buffErr(tw.Flush())
}

// humanSize renders a byte count in binary units, so a listing reads at a glance rather than
// in raw bytes. Sub-kibibyte sizes stay exact in bytes; larger ones round to one decimal of
// the largest unit that fits.
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

// shortTime renders an instant compactly for the listing, or a dash for a zero time.
func shortTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("2006-01-02 15:04")
}

// expiry renders an expiry instant, or the word never for a clip with no expiry — the zero
// time, which is the kept-forever sentinel and must not be shown as a date.
func expiry(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return t.Format("2006-01-02 15:04")
}

// flagText names the per-clip flags worth showing in the listing — consume-once and the
// executable bit — joined so a clip carrying both shows both, and rendered as a dash when it has
// neither so the column is never blank.
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
