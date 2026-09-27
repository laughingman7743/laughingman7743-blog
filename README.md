# laughingman7743-blog

Source for [blog.laughingman7743.org](https://blog.laughingman7743.org), built with [Zola](https://www.getzola.org/) and the [Apollo theme](https://github.com/not-matthias/apollo).
Posts live in [`content/posts/`](content/posts/).
The original publication dates are retained for the articles imported from Hatena Blog.
The Navidrome article is an English translation of the original Japanese post.

## Local development

Install Zola 0.23.2 and Node.js 24 or later, then clone the theme, install the build dependencies, and start a local server:

```sh
git submodule update --init --recursive
npm ci
zola serve
```

Check a production build before opening a pull request:

```sh
zola check --skip-external-links
npm run build
```

`zola serve` previews the content and theme.
`npm run build` also generates social preview images and their Open Graph and X Card metadata in `public/`.
Use the complete build for deployment.

The dependency override for Cheerio 1.2.0 selects `encoding-sniffer` 1.0.2 to avoid its deprecated `whatwg-encoding` dependency.
This build reads UTF-8 HTML with Cheerio's `load()` API; it does not use the buffer or stream encoding APIs.
Remove the override when upgrading to a Cheerio release that includes the updated dependency.

All changes go through pull requests.
Merging into `main` triggers the production deployment on Cloudflare Pages.

## Social previews

Every built page with a canonical URL receives a 1200 × 630 PNG containing its title, the blog name, and the production domain.
Article titles and descriptions come from the rendered page, so new posts need no image configuration.
The homepage, post index, and tag pages receive images too.
The renderer uses the Apollo theme's bundled Space Grotesk fonts without contacting an image service.
Long titles wrap onto up to five lines, with an ellipsis for overflow; the metadata retains the full title.
Titles with characters outside those fonts fail the build with a request to add font coverage.

Images are generated into `public/social/` and are not committed.
Their filenames include a hash of the PNG, so changes to the title or image design produce a new URL.
Social platforms can still cache the page metadata and control whether and when a card appears.
The layout and metadata generation live in `src/social-images.mjs`.

## Cloudflare Pages

Connect this GitHub repository to a Pages project with these build settings:

| Setting | Value |
| --- | --- |
| Production branch | `main` |
| Framework | Zola |
| Build command | `npm ci && npm run build` |
| Build output directory | `public` |
| Environment variable | `ZOLA_VERSION=0.23.2` |

For an existing Pages project, update the build command in **Settings → Build** before deploying this change.
The old `zola build` command does not generate social previews.
The `.node-version` file selects Node.js 24 in the Pages build environment.

The build uses `CF_PAGES_BRANCH` and `CF_PAGES_URL` to make links and social image URLs point to the pull request's preview deployment.
Canonical URLs in the HTML still point to the production domain.

Add `blog.laughingman7743.org` to the Pages project's **Custom domains** before creating its DNS record.
In Route 53, create a CNAME record named `blog` for `laughingman7743.org` that targets the Pages project's `<project-name>.pages.dev` hostname.
The domain's nameservers and the existing `navidrome.laughingman7743.org` records do not need to change.

The [migration map](MIGRATION.md) lists the old Hatena URLs and their new paths.
