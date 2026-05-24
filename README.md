# shuttle-go-web

ShuttleGo frontend static site for GitHub Pages.

## Included files

- `index.html`
- `auth-callback.html`
- `dev.config.js`
- `page/`
- `robots.txt`
- `sitemap.xml`
- Root image assets (`*.png`, `*.svg`)
- `CNAME`
- `.github/workflows/deploy-pages.yml`

## Deployment

Push SEO/site files to `main` or run the `Deploy GitHub Pages` workflow manually.

This site is configured for a root-hosted deployment and includes `CNAME=shuttle-go.com`.

## Backend API

- Production frontend code calls `https://cloud-api.shuttle-go.com`
- Development override lives in `dev.config.js`

If you publish under the default GitHub Pages repo path instead of a root custom domain, root-relative paths in the current HTML will need an additional base-path pass.
