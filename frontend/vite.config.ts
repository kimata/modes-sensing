import { execSync } from "child_process";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Git commit hash を取得
const getGitCommitHash = (): string => {
    try {
        return execSync("git rev-parse --short HEAD").toString().trim();
    } catch {
        return "unknown";
    }
};

// https://vitejs.dev/config/
export default defineConfig({
    plugins: [react()],
    base: "/modes-sensing",
    define: {
        "import.meta.env.VITE_BUILD_DATE": JSON.stringify(new Date().toISOString()),
        "import.meta.env.VITE_GIT_COMMIT_HASH": JSON.stringify(getGitCommitHash()),
    },
    build: {
        outDir: "dist",
    },
});
