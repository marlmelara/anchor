/** @type {import('next').NextConfig} */
const nextConfig = {
  // The viewer is read-only over the journal, so nothing here needs a server
  // action or a database connection of its own -- it talks to anchor-server.
  reactStrictMode: true,
};

export default nextConfig;
