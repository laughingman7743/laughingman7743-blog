import { createHash } from "node:crypto";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { Resvg } from "@resvg/resvg-js";
import { load } from "cheerio";
import satori from "satori";

const width = 1200;
const height = 630;

function element(children, style) {
  return { type: "div", props: { children, style } };
}

async function renderCard(title, siteName, domain, isArticle, fonts) {
  const card = element(
    [
      element(siteName, {
        fontSize: 25,
        color: "#9caebf",
      }),
      element(
        [
          element(isArticle ? "ENGINEERING NOTES" : "PERSONAL BLOG", {
            fontSize: 19,
            fontWeight: 700,
            letterSpacing: 4,
            color: "#7de2c4",
            marginBottom: 24,
          }),
          element(title, {
            display: "block",
            fontSize: title.length > 110 ? 48 : 60,
            fontWeight: 700,
            lineHeight: 1.15,
            letterSpacing: -1.5,
            lineClamp: 5,
            overflow: "hidden",
          }),
        ],
        { display: "flex", flexDirection: "column", width: "100%" },
      ),
      element(
        [
          element(domain, { fontSize: 22, color: "#9caebf" }),
          element("/", { fontSize: 30, color: "#7de2c4", fontWeight: 700 }),
        ],
        {
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          borderTop: "1px solid #304152",
          paddingTop: 22,
        },
      ),
    ],
    {
      display: "flex",
      flexDirection: "column",
      justifyContent: "space-between",
      width,
      height,
      padding: "48px 64px 38px",
      borderTop: "8px solid #7de2c4",
      backgroundColor: "#101a25",
      color: "#f3f6fa",
      fontFamily: "Space Grotesk",
    },
  );
  const svg = await satori(card, {
    width,
    height,
    fonts,
    loadAdditionalAsset: async (language, text) => {
      throw new Error(`Add a font for unsupported ${language} characters: ${text}`);
    },
  });
  return new Resvg(svg).render().asPng();
}

async function* htmlFiles(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const filename = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      yield* htmlFiles(filename);
    } else if (entry.isFile() && entry.name.endsWith(".html")) {
      yield filename;
    }
  }
}

function setMeta(document, attribute, name, value) {
  document(`head meta[${attribute}="${name}"]`).remove();
  document("head").append(
    document("<meta>").attr(attribute, name).attr("content", String(value)),
  );
}

export async function generateSocialImages(outputDirectory = "public") {
  const home = load(await readFile(path.join(outputDirectory, "index.html"), "utf8"));
  const siteName = home("title").text().replace(/\s+/g, " ").trim();
  const canonical = home('link[rel="canonical"]').attr("href");
  const domain = new URL(canonical).hostname;
  const fonts = await Promise.all(
    [["Regular", 400], ["Bold", 700]].map(async ([style, weight]) => ({
      name: "Space Grotesk",
      data: await readFile(`themes/apollo/static/fonts/SpaceGrotesk/SpaceGrotesk-${style}.ttf`),
      weight,
      style: "normal",
    })),
  );
  await mkdir(path.join(outputDirectory, "social"), { recursive: true });
  let count = 0;

  for await (const filename of htmlFiles(outputDirectory)) {
    const document = load(await readFile(filename, "utf8"));
    if (!document('link[rel="canonical"]').length) continue;

    const heading = document(".page-header").not("#modalTitle").first().text();
    const title = (heading || document("title").text()).replace(/\s+/g, " ").trim();
    const description = document('meta[property="og:description"]').attr("content") ?? "";
    const baseUrl = document('meta[name="base"]').attr("content");
    const isArticle = document("article").length > 0;
    const png = await renderCard(title, siteName, domain, isArticle, fonts);
    const digest = createHash("sha256").update(png).digest("hex").slice(0, 20);
    const imagePath = `social/${digest}.png`;
    const imageUrl = new URL(imagePath, `${baseUrl.replace(/\/$/, "")}/`).href;
    await writeFile(path.join(outputDirectory, imagePath), png);

    for (const [name, value] of Object.entries({
      "og:title": title,
      "og:type": isArticle ? "article" : "website",
      "og:site_name": siteName,
      "og:image": imageUrl,
      "og:image:type": "image/png",
      "og:image:width": width,
      "og:image:height": height,
      "og:image:alt": title,
    })) {
      setMeta(document, "property", name, value);
    }
    for (const [name, value] of Object.entries({
      "twitter:card": "summary_large_image",
      "twitter:title": title,
      "twitter:description": description,
      "twitter:image": imageUrl,
      "twitter:image:alt": title,
    })) {
      setMeta(document, "name", name, value);
    }
    await writeFile(filename, document.html());
    count += 1;
  }

  console.log(`Generated social preview images and metadata for ${count} pages.`);
}
