import { createElement } from "react-syntax-highlighter";
import { dwImageUrl, splitDwImgTokens } from "../../utils/dwImage";


function tokenTooltip(sha256: string, ref: string): string {
  return (
    "Stored image reference — the original image is held in your control plane " +
    `(content hash ${sha256.slice(0, 12)}…). Open to retrieve it via ` +
    `GET ${dwImageUrl(ref)} (management API; dashboard session or platform API key).`
  );
}

function linkifyNode(node: rendererNode): rendererNode | rendererNode[] {
  if (node.type === "text" && typeof node.value === "string") {
    const segments = splitDwImgTokens(node.value);
    if (segments.length === 1 && segments[0].kind === "text") {
      return node;
    }
    return segments.map((seg) =>
      seg.kind === "text"
        ? { type: "text", value: seg.value }
        : {
            type: "element",
            tagName: "a",
            properties: {
              className: ["dw-img-link"],
              href: dwImageUrl(seg.ref),
              target: "_blank",
              rel: "noreferrer noopener",
              title: tokenTooltip(seg.sha256, seg.ref),
              style: { textDecoration: "underline", cursor: "pointer" },
            },
            children: [{ type: "text", value: seg.raw }],
          },
    );
  }
  if (node.children) {
    return { ...node, children: node.children.flatMap(linkifyNode) };
  }
  return node;
}

export function dwImgLinkRenderer({ rows, stylesheet, useInlineStyles }: rendererProps) {
  return rows.map((row, i) =>
    createElement({
      node: { ...row, children: (row.children ?? []).flatMap(linkifyNode) },
      stylesheet,
      useInlineStyles,
      key: `dwimg-row-${i}`,
    }),
  );
}
