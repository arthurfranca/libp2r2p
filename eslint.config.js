import neostandard, { resolveIgnoresFromGitignore } from 'neostandard'
import globals from 'globals'

export default [
  ...neostandard({
    ignores: [...resolveIgnoresFromGitignore(), 'dist']
  }), {
    files: ['**/*.js'],
    languageOptions: {
      sourceType: 'module',
      globals: {
        ...globals.browser,
        ...globals.serviceworker,
        IS_DEVELOPMENT: 'readonly',
        IS_PRODUCTION: 'readonly'
      }
    },
    rules: {
      '@stylistic/comma-dangle': ['error', 'never'], // @stylistic/js/... isn't supported by neostandard yet
      '@stylistic/lines-between-class-members': 'off',
      '@stylistic/object-property-newline': 'off',
      'import/no-anonymous-default-export': 'off',
      'no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', caughtErrorsIgnorePattern: '^_', varsIgnorePattern: '^_$' }
      ],
      'one-var': 'off'
    }
  }
]
