# How to Work with Claude Code

This guide explains how to effectively use Claude Code (Anthropic's AI coding assistant) with this project.

## Key Files for AI-Assisted Development

### CLAUDE.md (Most Important)

Claude Code automatically reads this file when working in a project. Use it to tell Claude:

- **Build/test commands** - So Claude can verify changes work
- **Project structure** - Where things are located
- **Coding conventions** - Style preferences, patterns to follow
- **Context** - What the project does, key concepts

Keep this file updated as the project evolves. It's your primary way to give Claude persistent context.

### README.md

Standard project documentation. Claude reads this for general context about what the project does and how to use it.

### ARCHITECTURE.md

Describes system design, components, and how they interact. Helps Claude understand the big picture when making changes that span multiple areas.

### PLANS.md

Tracks what you're working on and what's coming next. Useful for giving Claude context about priorities and direction.

### DECISIONS.md

Records *why* certain choices were made. When Claude encounters a pattern and wonders "why is it done this way?", this file provides the answer. Prevents Claude from suggesting changes that would undo intentional decisions.

## Tips for Working with Claude Code

### Starting a Session

1. Claude automatically reads CLAUDE.md at the start
2. You can ask Claude to read other files for more context
3. Be specific about what you want to accomplish

### Giving Good Instructions

- **Be specific:** "Add a --verbose flag to the CLI" is better than "improve the CLI"
- **Provide context:** If there's relevant background, share it
- **Share constraints:** "Must work on Windows" or "Keep dependencies minimal"

### Iterating on Changes

- Review Claude's suggestions before accepting
- Ask for explanations if something is unclear
- Point out issues - Claude can fix mistakes
- Break large tasks into smaller steps

### Keeping Documentation Updated

After significant changes, ask Claude to update:
- CLAUDE.md if build commands or structure changed
- ARCHITECTURE.md if components were added/changed
- DECISIONS.md if important choices were made
- PLANS.md to mark items complete or add new ones

## Common Commands

```bash
# In Claude Code CLI
claude                    # Start interactive session
claude "your prompt"      # One-shot command
claude --help             # See all options
```

## Getting Help

- Type `/help` in Claude Code for built-in help
- See https://github.com/anthropics/claude-code for documentation
