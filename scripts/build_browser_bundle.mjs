// Build the browser-compatible JobPing bundles.
// Usage: node scripts/build_browser_bundle.mjs
//
// Outputs:
//   packages/js/dist/jobping_browser.mjs          (ESM, primary)
//   packages/js/dist/jobping_browser.min.js       (IIFE, minified, primary)
//   examples/experiment_group/jobping_browser.mjs (ESM, legacy symlink target)
//   examples/experiment_group/jobping_browser.min.js (IIFE, legacy)
import * as esbuild from "esbuild";
import { mkdirSync, copyFileSync } from "node:fs";

const distDir = "packages/js/dist";
const legacyDir = "examples/experiment_group";
mkdirSync(distDir, { recursive: true });
mkdirSync(legacyDir, { recursive: true });

const resolveIdBrowser = {
  name: "resolve-id-browser",
  setup(build) {
    build.onResolve({ filter: /\/id\.mjs$/ }, (args) => {
      return { path: args.resolveDir + "/id_browser.mjs" };
    });
  },
};

// ESM bundle
await esbuild.build({
  entryPoints: ["packages/js/browser_entry.mjs"],
  bundle: true,
  platform: "browser",
  format: "esm",
  plugins: [resolveIdBrowser],
  outfile: `${distDir}/jobping_browser.mjs`,
});

// Minified IIFE bundle
await esbuild.build({
  entryPoints: ["packages/js/browser_entry_min.mjs"],
  bundle: true,
  platform: "browser",
  format: "iife",
  globalName: "jobping",
  minify: true,
  plugins: [resolveIdBrowser],
  outfile: `${distDir}/jobping_browser.min.js`,
});

// Copy to legacy location for existing docs/examples
copyFileSync(`${distDir}/jobping_browser.mjs`, `${legacyDir}/jobping_browser.mjs`);
copyFileSync(`${distDir}/jobping_browser.min.js`, `${legacyDir}/jobping_browser.min.js`);

console.log(`Built ${distDir}/jobping_browser.mjs`);
console.log(`Built ${distDir}/jobping_browser.min.js`);
console.log(`Copied to ${legacyDir}/`);
