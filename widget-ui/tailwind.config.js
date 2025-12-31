/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        primary: '#3498db',
        secondary: '#72f951',
        background: '#f4f5f5',
        surface: '#ffffff',
        text: '#373739',
        'text-muted': '#97979b',
      },
      fontFamily: {
        sans: ['Blinker', 'Roboto', 'sans-serif'],
      }
    },
  },
  plugins: [],
}
