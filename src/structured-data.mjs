import { readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { load } from "cheerio";

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

// Apollo renders the article title as a <div>, which leaves the page without a
// meaningful <h1> (the only one belongs to the search modal).
function promoteTitleToHeading(document) {
  const header = document("article .title > .page-header").first();
  if (!header.length) return;
  header.replaceWith(
    document("<h1>").attr("class", header.attr("class")).text(header.text().trim()),
  );
}

function articleSchema(document) {
  const meta = (selector) => document(selector).attr("content")?.trim() || undefined;
  const url = document('link[rel="canonical"]').attr("href");
  const published = meta('meta[property="article:published_time"]');
  const authorName = meta('meta[name="author"]');
  const authorUrl = document('link[rel="author"]').attr("href");
  const tags = document("article .post-tag")
    .map((_, tag) => document(tag).text().trim())
    .get();

  return {
    "@context": "https://schema.org",
    "@type": "BlogPosting",
    headline: document("article .page-header").first().text().trim(),
    description: meta('meta[name="description"]'),
    url,
    mainEntityOfPage: { "@type": "WebPage", "@id": url },
    datePublished: published,
    dateModified: meta('meta[property="article:modified_time"]') ?? published,
    image: meta('meta[property="og:image"]'),
    inLanguage: document("html").attr("lang"),
    keywords: tags.length ? tags : undefined,
    author: authorName
      ? { "@type": "Person", name: authorName, url: authorUrl }
      : undefined,
  };
}

function serialize(data) {
  // Escape "<" so that a value containing "</script>" cannot close the element.
  return JSON.stringify(data).replace(/</g, "\\u003c");
}

export async function addStructuredData(outputDirectory = "public") {
  let count = 0;

  for await (const filename of htmlFiles(outputDirectory)) {
    const document = load(await readFile(filename, "utf8"));
    if (!document("article .title > .page-header").length) continue;

    promoteTitleToHeading(document);
    document('head script[type="application/ld+json"]').remove();
    document("head").append(
      document('<script type="application/ld+json"></script>').text(
        serialize(articleSchema(document)),
      ),
    );
    await writeFile(filename, document.html());
    count += 1;
  }

  console.log(`Added article headings and structured data to ${count} pages.`);
}
