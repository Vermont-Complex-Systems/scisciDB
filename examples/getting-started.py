# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "marimo>=0.17.0",
#     "pyzmq",
# ]
# ///

import marimo

__generated_with = "0.18.1"
app = marimo.App()


@app.cell
def _(mo):
    mo.md(r"""
    # Whirlwind tour of `scisciDB`

    In this tutorial, we show how we can use [marimo](https://marimo.io/) with [ducklake](https://ducklake.select/) to explore `scisciDB`, while keeping track of modifications made to the DB (via snapshots, time travel queries, schema evolution, and partitioning).

    ## Why ducklake?

    Some benefits are shared with the idea of [data lakehouses and catalogs](https://iceberg.apache.org/terms/):
    - `decoupling computing and storage`: this is a big idea with many ramifications I think academics could tremendously benefit from. To understand this feature, think about how relational tables store both metadata and data in the database. Data lakehouse, in contrasts, store both data and metadata in some form of file system storage layer, such as your local file systems, separated from each other (see [this ref](https://www.oracle.com/autonomous-database/apache-iceberg/)).
      - `scalability`: By separating computing from data itself, users the flexibility to choose the processing engine that is right for their specific needs.
      - `auditability`: For researchers, one interesting side effect of this decoupling is the ability to share the catalogs, providing the ability to navigate snapshots and time travel, independently of storage. This means we can test the result of every key operations (e.g. _auditability_), even while working with no-so-big datasets (<1TB).
      - `shareability`: For researchers, another benefit of this decoupling is the ability to share the catalogs to multiple collabortors while storing the data in one place. Within institution, this means we can one group storing the data, say on their netfiles, while other collaborators can access it from anywhere on the VACC (given they have the correct permissions).
        - `encrypted`: Stakeholders can submit encrypted parquet files on public storage system (S3, or perhaps even institutional storage), which can only be access with the private key from the catalog server. This can facilitate shareability across institutions and/or of sensitive data.

    But ducklake provides an additional layer of simplicity over other data lakehouses:
    - `transparency`: parquet files that we can inspect through and through.

    ## Intro ducklake (feauring marimo)
    """)
    return


@app.cell
def _():
    import marimo as mo
    import altair as alt
    return alt, mo


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        INSTALL ducklake;
        -- Here we use postgreSQL as datanase for `ducklake_catalog`
        ATTACH 'ducklake:../metadata.ducklake' AS scisciDB 
            (DATA_PATH '/netfiles/compethicslab/scisciDB/');
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## What are ducklake catalogs?
    """)
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SHOW ALL TABLES;
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- We can access those catalogs as any other table
        SELECT * FROM __ducklake_metadata_scisciDB.ducklake_table_stats;
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Lets take a look at `s2_papers`
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    If we count the total number of papers:
    """)
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT COUNT(*) as total_papers FROM scisciDB.s2_papers;
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    We can describe it using `DESCRIBE`
    """)
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE SELECT * FROM scisciDB.s2_papers;
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT * FROM scisciDB.s2_papers LIMIT 5;
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Looking at text availability
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    Now, say that we want to count number of papers by year
    """)
    return


@app.cell
def _(alt, mo):
    count_df = mo.sql("""
    SELECT 
        COUNT(*) as n, year 
        FROM scisciDB.s2_papers 
        WHERE 
            year IS NOT NULL AND year > 1900
        GROUP BY year;
    """)

    alt.Chart(count_df).mark_bar().encode(
        x=alt.X('year:O', 
                axis=alt.Axis(labelAngle=-45, 
                             values=list(count_df['year'][::5]))),  # Show every 5th year
        y=alt.Y('n:Q', title='Count'),
        tooltip=['year:O', 'n:Q']
    ).properties(
        width=700,
        height=200
    ).configure_axis(
        grid=True,
        gridOpacity=0.3
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    Huzzay. I cannot overemphasize enough how it used to be more messy than that to do this count.

    How many of those have parsed text? This seems like a simple question.

    Here we use symlog because there are years for which $n=0$
    """)
    return


@app.cell
def _(alt, mo):
    fulltext_df = mo.sql("""
    SELECT 
        year,
        COUNT(*) as total_papers,
        COUNT(*) FILTER (WHERE has_fulltext) as papers_with_fulltext,
        COUNT(*) FILTER (WHERE NOT has_fulltext) as papers_without_fulltext
    FROM scisciDB.s2_papers 
    WHERE year IS NOT NULL AND year > 1900
    GROUP BY year
    ORDER BY year;
    """).to_pandas()  

    # Create base chart
    base = alt.Chart(fulltext_df).encode(
        x=alt.X('year:Q',
                axis=alt.Axis(labelAngle=-45, values=list(fulltext_df['year'][::5])),
                title='Year')
    )

    # Bottom bar (total) - orange
    bottom = base.mark_bar(color='orange', opacity=0.8).encode(
        y=alt.Y('total_papers:Q',
                scale=alt.Scale(type='symlog'),
                title='Number of Papers'),
        tooltip=['year', 'total_papers']
    )

    # Top bar (with fulltext) - blue
    top = base.mark_bar(color='steelblue', opacity=0.8).encode(
        y=alt.Y('papers_with_fulltext:Q',
                scale=alt.Scale(type='symlog')),
        tooltip=['year', 'papers_with_fulltext']
    )

    (bottom + top).properties(
        width=800,
        height=300,
        title='Full Text Availability by Year'
    ).configure_axis(
        grid=True,
        gridOpacity=0.3
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    Duckdb is also nice because we can easly examine what's happening under the hood
    (we did need to do a little hack to see it nicely formated in html):
    """)
    return


@app.cell
def _(mo):
    plan = mo.sql("""
    EXPLAIN ANALYZE
    SELECT 
        year,
        COUNT(*) as total_papers,
        COUNT(*) FILTER (WHERE has_fulltext) as papers_with_fulltext,
        COUNT(*) FILTER (WHERE NOT has_fulltext) as papers_without_fulltext
    FROM scisciDB.s2_papers 
    WHERE year IS NOT NULL AND year > 1900
    GROUP BY year
    ORDER BY year;
    """).to_pandas()

    mo.Html(f"""
    <div style="
        font-family: 'Courier New', monospace; 
        font-size: 12px; 
        white-space: pre-wrap;
        word-wrap: break-word;
        max-width: 600px;
        max-height: 600px; 
        overflow-x: auto; 
        overflow-y: auto; 
    ">
    {plan.iloc[0,1]}
    </div>
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Looking at citation counts
    """)
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SUMMARIZE (SELECT citationcount FROM scisciDB.s2_papers);
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT 
            journal.name,
            SUM(TRY_CAST(citationcount AS INTEGER)) as total_citations
        FROM scisciDB.s2_papers
        WHERE journal.name IS NOT NULL
        GROUP BY journal.name
        ORDER BY total_citations DESC;
        """
    )
    return


@app.cell
def _(mo):
    journal_multi = mo.ui.multiselect(
        options=['Nature', 'Science', 'Cell', 'PLOS ONE', 'Proceedings of the National Academy of Sciences of the United States of America'],
        value=['Nature', 'Science', 'Proceedings of the National Academy of Sciences of the United States of America', 'Cell'],
        label='Select Journals:'
    )

    journal_multi
    return (journal_multi,)


@app.cell
def _(alt, journal_multi, mo):
    results_multi = mo.sql(
        f"""
        SELECT 
            journal.name as journal,
            year, 
            AVG(citationcount) AS avg_citation_count
        FROM scisciDB.s2_papers 
        WHERE journal.name IN {tuple(journal_multi.value)}
          AND year BETWEEN 2000 AND 2024
        GROUP BY journal.name, year 
        ORDER BY year;
        """
    )

    alt.Chart(results_multi).mark_line(point=True).encode(
        x=alt.X('year:O', title='Year'),
        y=alt.Y('avg_citation_count:Q', title='Average Citation Count'),
        color=alt.Color('journal:N', title='Journal'),
        tooltip=['journal', 'year', 'avg_citation_count']
    ).properties(
        title='Average Citations Over Time - Comparison',
        width=700,
        height=400
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""

        """
    )
    return


if __name__ == "__main__":
    app.run()
