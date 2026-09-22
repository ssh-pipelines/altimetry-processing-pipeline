# Data Flow — NASA-SSH Products

**Scope:** the **reference-mission** and **high latitude** along-track products and the downstream products that take them as input.


The along-track stages (`daily_files` → `xover` → `oer` → `xover` → `bad_pass` →
`finalizer`) are generic — *every* source runs its own independent instance of
them. Reference-mission sources continue into the **unifier**, which exists to 
assemble the reference-mission record; high-latitude sources never enter it. So the
two families end differently: many reference-mission sources collapse into one
stitched product, while each high-latitude satellite publishes its own.


```mermaid
flowchart TB
    subgraph UP["Reference-mission runs — PO.DAAC granules via CMR"]
        direction LR
        GSFC["<b>GSFC</b><br/>1992-10-25 → 2025-12-31"]
        S6["<b>S6</b><br/>2026-01-01 →"]
        S6B["<b>S6B</b><br/>TBD → TBD"]
    end

    subgraph UA["High-latitude runs — AVISO granules via THREDDS, plus the reference mission"]
        direction TB
        HY2B["<b>HY-2B</b><br/>TBD → TBD"]
        S3B["<b>S3B</b><br/>2018-11-24 → 2026-04-10"]
        S3A["<b>S3A</b><br/>TBD → TBD"]
        SARAL["<b>Saral</b><br/>TBD → TBD"]
        HY2A["<b>HY-2A</b><br/>TBD → TBD"]
        CRYO["<b>Cryosat-2</b><br/>TBD → TBD"]
        ENVI["<b>Envisat</b><br/>TBD → TBD"]
        ERS2["<b>ERS-2</b><br/>TBD → TBD"]
        ERS1["<b>ERS-1</b><br/>TBD → TBD"]
        NSSHREF[/"NASA-SSH reference-mission along-track product<br/><i>required for crossovers</i>"/]

        HY2B ~~~ SARAL
        SARAL ~~~ ENVI

        S3B ~~~ HY2A
        HY2A ~~~ ERS2

        S3A ~~~ CRYO
        CRYO ~~~ ERS1

        ENVI ~~~ NSSHREF
        ERS2 ~~~ NSSHREF
        ERS1 ~~~ NSSHREF
    end

    UP --> AT
    UA --> AT

    subgraph AT["Along-track product pipeline — one independent run per source"]
        direction TB

        DF["<i>daily file generation</i><br/>MSS swapped, ssha smoothed, nasa_ssh flag derivation"]
        DF ==> P1[/"P1 — daily along track file"/]
        P1 ==> OER["<i>oer</i><br/>apply correction"]
        OER ==> P2[/"P2 — OER-corrected daily along track file"/]
        P2 ==> FIN["<i>finalizer</i><br/>apply absolute offset, flag bad passes"]
        FIN ==> P3[/"P3 — finalized daily file"/]

        P1 --> X1["<i>xover</i>"]
        X1 --> XO1[/"p1 crossovers"/]
        XO1 -->|"stats fit the correction"| OER

        P2 --> X2["<i>xover</i>"]
        X2 --> XO2[/"p2 crossovers"/]
        XO2 --> BP["<i>bad pass flagging</i><br/>stats vs. thresholds"]
        BP -->|"which passes to flag"| FIN
    end

    subgraph HILAT["High-latitude product lines; not yet in production"]
        direction TB
        HLP[/"NASA-SSH high-latitude along-track product<br/><i>one per source</i>"/]
    end

    subgraph REF["Reference-mission product line"]
        direction TB
        UNIF["<i>unifier</i><br/>reference-mission product selection"]
        UNIF ==> NSSH[/"NASA-SSH reference-mission along-track product"/]
    end

    P3 -.->|"high-latitude sources"| HLP
    P3 -->|"reference-mission sources"| UNIF

    NSSH --> SGS

    subgraph SG["Gridded product pipeline"]
        direction TB

        SGS["<i>simple grids</i><br/>basin-aware Gaussian resample to 0.5°"]
        SGS ==> SGP[/"NASA-SSH reference-mission simple grid product"/]

        SGP --> ENSO["<i>enso grids</i><br/>smooth, deseason, detrend, → 0.25°"]
        ENSO ==> EM[/"ENSO maps<br/>ortho + plate PNGs for websites"/]

        SGP --> IND["<i>indicators</i><br/>GMSL, ENSO, PDO, IOD"]
        IND ==> INDP[/"NASA-SSH indicators products"/]
    end

    classDef upstream fill:#fdeae1,stroke:#eb6834,stroke-width:1.5px,color:#0b0b0b
    classDef stage fill:#cde2fb,stroke:#2a78d6,stroke-width:1.5px,color:#0b0b0b
    classDef internal fill:#f0efec,stroke:#8a8985,stroke-width:1px,color:#0b0b0b
    classDef published fill:#d7f2e7,stroke:#1baf7a,stroke-width:2px,color:#0b0b0b
    classDef disabled fill:#e6e4e1,stroke:#a6a09a,stroke-width:1.5px,stroke-dasharray:5 3,color:#615c57
    classDef published_disabled fill:#eaf5ee,stroke:#8cc4a5,stroke-width:2px,stroke-dasharray:5 3,color:#3f4b45

    class GSFC,S6 upstream
    class DF,OER,FIN,X1,X2,BP,UNIF,SGS,ENSO,IND stage
    class P1,P2,XO1,XO2,P3 internal
    class NSSH,NSSHREF,SGP,EM,INDP published

    %% Not enabled yet. Sources drain to grey — safe, since every grey node
    %% elsewhere is a parallelogram. HLP keeps a faded green instead: a grey
    %% parallelogram would read as an intermediate file rather than a product.
    class S3B,S6B,S3A,HY2B,SARAL,CRYO,HY2A,ENVI,ERS1,ERS2 disabled
    class HLP published_disabled
```

**Reading the diagram.** Blue = a processing stage; gray = an intermediate file;
green = a published product; orange = upstream granules. Rectangles are stages,
parallelograms are files; the boxes are grouping only.

A **dashed border means not enabled yet**, and a dashed edge marks a branch that
is not live. Color still carries the role, so the inactive high-latitude product
stays green — faded, but a product rather than an intermediate file. GSFC and S6
are the only sources enabled today, which is why the entire high-latitude branch
is dashed.

**Required inputs** is grouped by which family of run consumes them, which is why
one input is green: a high-latitude run needs the pipeline's own reference-mission
product, already published, to compute its crossovers against — it has no
same-mission passes of its own to cross (see
[ADR-0006](adr/0006-reference-crossovers-against-nasa-ssh-p3.md)). That product
therefore appears twice: as an input at the top, as an output below.

In the along-track pipeline, the bold chain is the daily file as each stage
rewrites it. The side branches compute the statistics that drive those rewrites —
crossover stats set the OER correction, bad-pass stats decide which passes get
flagged.
