/**
 * Command Loader — scans custom slash commands from Claude Code command directories.
 *
 * Loads commands from:
 * 1. ~/.claude/commands/*.md (global user commands)
 * 2. <project>/.claude/commands/*.md (project-level commands)
 *
 * These are the same directories that Claude Code CLI uses for custom slash commands.
 * Commands are loaded as prompt templates that get forwarded to the conversation engine.
 *
 * Results are cached in memory with a TTL to avoid repeated filesystem scans.
 */

import fs from 'fs';
import path from 'path';
import os from 'os';

/** A loaded custom command definition (immutable). */
export interface CustomCommand {
  /** Command name without leading slash (e.g. "commit", "review:pr") */
  readonly name: string;
  /** Telegram-safe command name (colons → underscores, lowercase, alphanumeric only) */
  readonly telegramName: string;
  /** Short description extracted from the first line of the .md file */
  readonly description: string;
  /** Full prompt template content */
  readonly content: string;
  /** Where this command was loaded from */
  readonly source: 'global' | 'project';
  /** Absolute path to the .md file */
  readonly filePath: string;
}

/**
 * Built-in bridge commands that should never be overridden by custom commands.
 */
const RESERVED_COMMANDS = new Set([
  'start', 'new', 'bind', 'cwd', 'mode',
  'status', 'sessions', 'stop', 'help',
]);

/** Cache entry with TTL tracking. */
interface CacheEntry {
  readonly commands: readonly CustomCommand[];
  readonly expiresAt: number;
}

/** In-memory cache keyed by working directory (or '__global__' for no-project calls). */
const cache = new Map<string, CacheEntry>();

/** Cache TTL in milliseconds (30 seconds). */
const CACHE_TTL_MS = 30_000;

/**
 * Get the global commands directory path.
 */
function getGlobalCommandsDir(): string {
  return path.join(os.homedir(), '.claude', 'commands');
}

/**
 * Get the project-level commands directory path.
 */
function getProjectCommandsDir(workingDirectory?: string): string {
  return path.join(workingDirectory || process.cwd(), '.claude', 'commands');
}

/**
 * Convert a command name to a Telegram-compatible command name.
 * Colons → underscores, lowercase, strip non-alphanumeric/underscore chars.
 */
function toTelegramName(name: string): string {
  return name.replace(/:/g, '_').toLowerCase().replace(/[^a-z0-9_]/g, '');
}

/**
 * Recursively scan a directory for .md command files.
 * Subdirectories create colon-separated command names (e.g. review/pr.md → "review:pr").
 */
function scanDirectory(
  dir: string,
  source: 'global' | 'project',
  prefix = '',
): readonly CustomCommand[] {
  if (!fs.existsSync(dir)) return [];

  const commands: CustomCommand[] = [];

  try {
    const entries = fs.readdirSync(dir, { withFileTypes: true });

    for (const entry of entries) {
      if (entry.name.startsWith('.')) continue;

      const fullPath = path.join(dir, entry.name);

      if (entry.isDirectory()) {
        const subPrefix = prefix ? `${prefix}:${entry.name}` : entry.name;
        commands.push(...scanDirectory(fullPath, source, subPrefix));
        continue;
      }

      if (!entry.name.endsWith('.md')) continue;

      const baseName = entry.name.replace(/\.md$/, '');
      const name = prefix ? `${prefix}:${baseName}` : baseName;

      // Skip reserved bridge commands
      if (RESERVED_COMMANDS.has(name)) continue;

      const content = fs.readFileSync(fullPath, 'utf-8');
      const firstLine = content.split('\n')[0]?.trim() || '';
      const description = firstLine.startsWith('#')
        ? firstLine.replace(/^#+\s*/, '')
        : firstLine || `Custom command: /${name}`;

      commands.push({
        name,
        telegramName: toTelegramName(name),
        description: description.slice(0, 256),
        content,
        source,
        filePath: fullPath,
      });
    }
  } catch (err: unknown) {
    // Log permission errors specifically to aid troubleshooting
    if (err instanceof Error && 'code' in err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === 'EACCES' || code === 'EPERM') {
        console.warn(`[command-loader] Permission denied reading ${dir}: ${err.message}`);
      }
    }
    // Other errors (e.g. directory vanished between exists check and read) are silently ignored
  }

  return commands;
}

/**
 * Load all custom commands from global and project directories.
 *
 * Project commands take priority over global commands with the same name.
 * Results are cached in memory with a 30-second TTL to avoid repeated I/O.
 */
export function loadCustomCommands(workingDirectory?: string): readonly CustomCommand[] {
  const cacheKey = workingDirectory || '__global__';
  const now = Date.now();

  const cached = cache.get(cacheKey);
  if (cached && cached.expiresAt > now) {
    return cached.commands;
  }

  const globalDir = getGlobalCommandsDir();
  const projectDir = getProjectCommandsDir(workingDirectory);

  const globalCommands = scanDirectory(globalDir, 'global');
  const projectCommands = scanDirectory(projectDir, 'project');

  // Project commands override global ones with the same name
  const projectNames = new Set(projectCommands.map(c => c.name));
  const dedupedGlobal = globalCommands.filter(c => !projectNames.has(c.name));

  const result = [...projectCommands, ...dedupedGlobal];
  cache.set(cacheKey, { commands: result, expiresAt: now + CACHE_TTL_MS });

  return result;
}

/**
 * Find a custom command by its original name or Telegram-safe name.
 * Searches project commands first, then global commands.
 */
export function findCustomCommand(
  name: string,
  workingDirectory?: string,
): CustomCommand | null {
  const commands = loadCustomCommands(workingDirectory);

  // Exact match on original name
  const exact = commands.find(c => c.name === name);
  if (exact) return exact;

  // Match on Telegram-safe name (handles underscore↔colon mapping)
  const byTelegram = commands.find(c => c.telegramName === name);
  if (byTelegram) return byTelegram;

  return null;
}

/**
 * Check if a command name is a reserved bridge command.
 */
export function isReservedCommand(name: string): boolean {
  return RESERVED_COMMANDS.has(name);
}

/**
 * Clear the command cache. Useful for testing or when commands have changed.
 */
export function clearCommandCache(): void {
  cache.clear();
}
