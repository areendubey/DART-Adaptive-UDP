# AI Use Disclosure — Selective Repeat module

**Author:** Savvy Jatrana  |  **Tool:** Claude (Anthropic)
**My part:** Selective Repeat sender/receiver (`selective_repeat.py`), its tests, and related notes.

I used Claude for help with my part only. Below are the prompts I gave and why. I wrote Prompt 1's
entry myself; Claude drafted the rest from my chat history and I checked them against what I did.


---

## Prompt 1
> "hi i am savvy give me step by step how to complete my part and also give the code for each step
> i need to do and explain it in simple terms ...make sure to keep it such that later i am able to
> integrate the things with my other team members ....pls keep the code optimized and easy to understand"

**Why:** I knew Selective Repeat from class but had never built a full ARQ with per-packet timers
and a reorder buffer. I wanted it broken into steps I could understand and explain in the viva,
and code that would plug into my teammates' parts.

**What I did:** Read all the code , and did a real localhost UDP transfer checking the
SHA-256 hashes matched.

---

## Prompt 2
*(Attached our project proposal PDF.)*
> "so i have all the supporting codes for this project i am savvy i need to perform my share i want
> you to give me the main code for the selective repeat part impelemtation testing make it optimized"

**Why:** The proposal gives me the Selective Repeat implementation. I attached it so the code would
match what we promised (checksums, timers, Karn's algorithm, SHA-256 check, seeded experiments).

**What I did:** Got a first version of `selective_repeat.py` and its tests. It used its own packet
format since Claude hadn't seen the team's code, so I treated it as a starting point.

---

## Prompt 3
*(Attached the team's `cli.py` and `dev_channel.py`.)*
> "change the name of the files you gave before To selective repeat . Py and test _selective_repeat.py
> and the cli or dev files i gave you rn should Should import same file give all the 4 files in correct way"

**Why:** The first version didn't fit our repo. `cli.py` and `dev_channel.py` import `SRSender` and
`SRReceiver` from `selective_repeat` and only give the ARQ `sendto`, `recvfrom` and `settimeout`.

**What I did:** Got a rewritten module with those class names and socket interface; `cli.py` and
`dev_channel.py` stayed unchanged. I checked the imports and re-ran everything.

---

## Prompt 4
*(Attached the team's `config.py` and `rto.py`.)*
> "take these config and rto and do it properly"

**Why:** Claude had to guess our `Config`, `Stats` and RTO interfaces before. With the real files, I
wanted my code to follow them exactly so the logger, the adaptive engine and the RTO experiments work
without changes.

**What I did:** Got the module rebuilt on the real `Config`, `Stats` and RTO methods, with 38 tests
(one test also found and fixed a bug where a lost final ACK left the sender unconfirmed). I went
through `_on_ack`, `_expire` and the FIN/linger code so I can explain them, and ran the suite myself.

---

## Prompt 5
> "how do i run a test suite for the whole of my part"

**Why:** I wanted to run all my tests myself for the demo and after every change.

**What I did:** Used `python -m unittest -v test_selective_repeat` and checked it passed.

---

## What the AI did not do
- Design the adaptive engine, packet header, channel emulator, or any teammate's part.
- Write `config.py`, `rto.py`, `cli.py` or `dev_channel.py` — those are my teammates' files.
- Decide whether my tests pass — I ran them myself (Claude's own sandbox runs don't count).
- Push to GitHub or open pull requests — I did that under my own account.


---

## 5. Formal Declaration

I hereby confirm that this record represents an honest and thorough disclosure of all AI assistance used in the development of my assigned components for DART. All core algorithms, state transition mechanisms, packet serialization, and architectural integrations are my own original work.

**Signed:**

Savvy Jatrana
Roll No: 24051046
Department of Computer Science & Engineering
Kalinga Institute of Industrial Technology (KIIT)
