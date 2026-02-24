import type { Config } from 'tailwindcss';

const config: Config = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        hologram: {
          blue: '#00d4ff',
          gold: '#ffd700',
          red: '#ff3333',
          dark: '#0a0a0a',
        },
      },
    },
  },
  plugins: [],
};

export default config;
