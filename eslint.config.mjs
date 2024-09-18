import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import globals from 'globals';
import prettierRecommended from 'eslint-plugin-prettier/recommended';

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  prettierRecommended,
  {
    ignores: ['**/*.cjs', '**/coverage', '**/public', '**/dist', '**/pnpm-lock.yaml', '**/pnpm-workspace.yaml'],
  },
  {
    languageOptions: {
      // parser: typescriptParser,
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: {
        ...globals.node,
      },
    },

    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
      // allow unused variables if they start with underscore
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          caughtErrorsIgnorePattern: '_',
        },
      ],
    },
  },
);
