# Chapter 2: Overall Program Structure

This chapter explains how Loom programs are structured from script text to runtime behavior.

You will learn:
- What parts make up a Loom program.
- How execution moves from parse to validation to runtime.
- How to organize files and boundaries so scripts stay maintainable.

Loom is designed so file workflows read like data flow diagrams, not like plumbing code.

That structure is not just style. It affects safety, debuggability, and team velocity.

A useful mental model:
1. Source text (`.loom`) describes intent.
2. Parser turns text into a structured program model.
3. Validator catches semantic issues early.
4. Runtime executes flows and applies policy rules.
5. Output is produced (files, logs, side effects, and errors).

By the end of this chapter, you should be able to look at a Loom script and explain what happens, in what order, and under which constraints.
