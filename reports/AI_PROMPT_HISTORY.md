# AI Prompt History and Usage Disclosure

## 1. AI Tool Used

**Tool:** ChatGPT

ChatGPT was used as an AI-assisted development tool during the development of this event-processing pipeline. It helped with understanding implementation approaches, debugging, test planning, and preparing project documentation.

## 2. Note on Prompt History

The original prompts were not saved as a separate transcript during development. Therefore, the entries below are reconstructed summaries of the main topics discussed with ChatGPT, not verbatim copies of the original prompts.

## 3. Reconstructed Prompt History

### A. Pipeline Design and Implementation
**Prompt topic:** Understanding how to structure an event-processing pipeline using Python, JSONL input, and SQLite storage.

**How AI helped:** ChatGPT provided guidance on breaking the pipeline into logical stages and considering data validation, persistence, and processing behavior.

**My review:** I reviewed the implementation and ran the project locally to verify its behavior.

### B. Error Handling and Data Quality
**Prompt topic:** Handling malformed records, duplicate events, invalid data, and late-arriving events.

**How AI helped:** ChatGPT assisted with reasoning about edge cases and approaches for testing failure scenarios.

**My review:** I ran the automated tests and examined the generated reports to check the observed processing results.

### C. Testing and Reliability
**Prompt topic:** Identifying useful test cases for event processing, checkpoint recovery, duplicate handling, and failure scenarios.

**How AI helped:** ChatGPT assisted with test planning, debugging, and interpreting test results.

**My validation:** I executed `python -m unittest discover -s tests -v`. The test suite completed with 31 passing tests.

### D. Documentation and Reporting
**Prompt topic:** Organizing the README, data dictionary, design trade-offs, benchmark explanation, and final run report.

**How AI helped:** ChatGPT assisted with structuring and improving the clarity of technical documentation.

**My review:** I reviewed the documentation and included actual output from a local pipeline run in the final report.

### E. Git and Repository Preparation
**Prompt topic:** Organizing project files and preparing documentation for a public GitHub repository.

**How AI helped:** ChatGPT provided guidance on repository organization and Git commands.

**My validation:** I committed and pushed project files to the GitHub repository and checked the reported Git status.

## 4. Where I Relied on AI

I relied on ChatGPT for development guidance, debugging assistance, test ideas, explanations of technical concepts, and documentation support. AI suggestions were treated as assistance rather than automatic proof that the implementation was correct.

## 5. What I Personally Reviewed and Validated

- Ran the Python project locally.
- Executed the automated test suite and observed 31 passing tests.
- Examined pipeline output and reconciliation results.
- Checked that the reported total event count matched the distinct event ID count in the recorded run.
- Reviewed project files and pushed changes to GitHub.

## 6. Limitations and Responsibility

The reconstructed entries above are summaries, not an exact prompt transcript. They should not be interpreted as a complete record of every interaction.

I remain responsible for understanding the implementation, verifying its behavior, explaining design decisions, and acknowledging limitations. I will clarify any implementation details I cannot independently explain during a technical walkthrough.
