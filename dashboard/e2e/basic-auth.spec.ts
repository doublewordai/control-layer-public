import { test, expect } from "@playwright/test";
import { AuthHelper } from "./auth.helper";

const adminEmail = process.env.ADMIN_EMAIL || "yicheng@doubleword.ai";

test.describe("Authentication Flow", () => {
  test("should redirect unauthenticated users to login", async ({ page }) => {
    await page.goto("/");

    await expect(page).toHaveURL(/accounts\.google\.com/);
  });

  test("admin should have full dashboard access", async ({ page }) => {
    const auth = new AuthHelper(page);

    await auth.loginAsAdmin();
    await page.goto("/");

    await expect(page).toHaveURL(/^https:\/\/localhost\/(models)?$/);

    await expect(
      page.getByRole("button").filter({ hasText: adminEmail }),
    ).toBeVisible();

    await expect(page.getByRole("link", { name: /models/i })).toBeVisible();
    await expect(page.getByRole("link", { name: /playground/i })).toBeVisible();
    await expect(page.getByRole("link", { name: /api keys/i })).toBeVisible();
    await expect(page.getByRole("link", { name: /settings/i })).toBeVisible();

    await expect(
      page.getByRole("link", { name: /users.*groups/i }),
    ).toBeVisible();
    await expect(page.getByRole("link", { name: /endpoints/i })).toBeVisible();
  });

  test("regular user should have limited dashboard access", async ({
    page,
  }) => {
    const auth = new AuthHelper(page);

    await auth.loginAsUser();
    await page.goto("/");

    await expect(page).toHaveURL(/^https:\/\/localhost\/(models)?$/);

    await expect(
      page.getByRole("button").filter({ hasText: "user@example.org" }),
    ).toBeVisible();

    await expect(page.getByRole("link", { name: /models/i })).toBeVisible();
    await expect(page.getByRole("link", { name: /playground/i })).toBeVisible();
    await expect(page.getByRole("link", { name: /api keys/i })).toBeVisible();

    await expect(
      page.getByRole("link", { name: /settings/i }),
    ).not.toBeVisible();
    await expect(
      page.getByRole("link", { name: /users.*groups/i }),
    ).not.toBeVisible();
    await expect(
      page.getByRole("link", { name: /endpoints/i }),
    ).not.toBeVisible();
  });
});
