// Build the browser-compatible JobPing bundle.
// Usage: node scripts/build_browser_bundle.mjs
import * as esbuild from "esbuild";

const resolveIdBrowser = {
  name: "resolve-id-browser",
  setup(build) {
    build.onResolve({ filter: /\/id\.mjs$/ }, (args) => {
      return { path: args.resolveDir + "/id_browser.mjs" };
    });
  },
};

await esbuild.build({
  entryPoints: ["packages/js/browser_entry.mjs"],
  bundle: true,
  format: "esm",
  outfile: "examples/experiment_group/jobping_browser.mjs",
  plugins: [resolveIdBrowser],
});

console.log("Built examples/experiment_group/jobping_browser.mjs");
