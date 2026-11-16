import { Page } from "@playwright/test";
import { execSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export class AuthHelper {
  private adminEmail: string;

  constructor(private page: Page) {
    this.adminEmail = process.env.ADMIN_EMAIL || "yicheng@doubleword.ai";
  }

  async loginWithJWT(email: string) {
    const jwt = this.generateJWT(email);

    await this.page.context().addCookies([
      {
        name: "dwctl_session",
        value: jwt,
        domain: "localhost",
        path: "/",
        secure: true,
        httpOnly: true,
      },
    ]);
  }

  async loginAsAdmin() {
    await this.loginWithJWT(this.adminEmail);
  }

  async loginAsUser() {
    await this.loginWithJWT("user@example.org");
  }

  async logout() {
    await this.page.context().clearCookies();
  }

  private generateJWT(email: string): string {
    try {
      const scriptPath = path.resolve(
        __dirname,
        "../../../scripts/generate-jwt.sh",
      );

      const jwtSecret =
        process.env.JWT_SECRET ||
        "dev-insecure-jwt-secret-change-in-production";

      const jwt = execSync(
        `JWT_SECRET="${jwtSecret}" ${scriptPath} "${email}"`,
        {
          encoding: "utf8",
          stdio: ["pipe", "pipe", "pipe"],
        },
      ).trim();

      return jwt;
    } catch (error) {
      throw new Error(`Failed to generate JWT for ${email}: ${error}`);
    }
  }
}
