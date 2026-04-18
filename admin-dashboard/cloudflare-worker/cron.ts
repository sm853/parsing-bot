export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const url = `${env.ADMIN_BASE_URL}/api/admin/refresh`;
    const res = await fetch(url, {
      method: 'POST',
      headers: { Authorization: `Bearer ${env.ADMIN_SECRET}` },
    });
    const body = await res.json();
    console.log('Daily stats refresh:', body);
  },
} satisfies ExportedHandler<Env>;

interface Env {
  ADMIN_BASE_URL: string;
  ADMIN_SECRET: string;
}
