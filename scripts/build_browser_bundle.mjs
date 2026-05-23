// Build the browser-compatible JobPing bundles.
// Usage: node scripts/build_browser_bundle.mjs
//
// Outputs:
//   examples/experiment_group/jobping_browser.mjs     (ESM, debug)
//   examples/experiment_group/jobping_browser.min.js   (IIFE, minified)
import * as esbuild from "esbuild";

const resolveIdBrowser = {
  name: "resolve-id-browser",
  setup(build) {
    build.onResolve({ filter: /\/id\.mjs$/ }, (args) => {
      return { path: args.resolveDir + "/id_browser.mjs" };
    });
  },
};

const baseConfig = {
  entryPoints: ["packages/js/browser_entry.mjs"],
  bundle: true,
  plugins: [resolveIdBrowser],
};

// ESM bundle (for module-aware consumers)
await esbuild.build({
  ...baseConfig,
  format: "esm",
  outfile: "examples/experiment_group/jobping_browser.mjs",
});

// Minified IIFE bundle (for <script> tag inclusion, exposes global `jobping`).
// Uses a separate entry that avoids top-level await.
await esbuild.build({
  entryPoints: ["packages/js/browser_entry_min.mjs"],
  bundle: true,
  format: "iife",
  globalName: "jobping",
  minify: true,
  plugins: [resolveIdBrowser],
  outfile: "examples/experiment_group/jobping_browser.min.js",
});

console.log("Built examples/experiment_group/jobping_browser.mjs");
console.log("Built examples/experiment_group/jobping_browser.min.js");
