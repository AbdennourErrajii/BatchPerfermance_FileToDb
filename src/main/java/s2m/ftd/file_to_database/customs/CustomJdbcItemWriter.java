package s2m.ftd.file_to_database.customs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcParameterUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.Assert;
import s2m.ftd.file_to_database.abstracts.AbstractItemWriter;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CustomJdbcItemWriter<T> extends AbstractItemWriter<T> {

    protected static final Log logger = LogFactory.getLog(CustomJdbcItemWriter.class);

    private NamedParameterJdbcOperations namedParameterJdbcTemplate;
    private String sql;
    private ItemPreparedStatementSetter<T> itemPreparedStatementSetter;
    private ItemSqlParameterSourceProvider<T> itemSqlParameterSourceProvider;
    private int parameterCount;
    private boolean usingNamedParameters;

    public CustomJdbcItemWriter(DataSource dataSource, String sql) {
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.sql = sql;
    }

    // Setter for ItemPreparedStatementSetter
    public void setItemPreparedStatementSetter(ItemPreparedStatementSetter<T> itemPreparedStatementSetter) {
        this.itemPreparedStatementSetter = itemPreparedStatementSetter;
    }

    // Setter for ItemSqlParameterSourceProvider
    public void setItemSqlParameterSourceProvider(ItemSqlParameterSourceProvider<T> itemSqlParameterSourceProvider) {
        this.itemSqlParameterSourceProvider = itemSqlParameterSourceProvider;
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(namedParameterJdbcTemplate, "A DataSource or a NamedParameterJdbcTemplate is required.");
        Assert.notNull(sql, "An SQL statement is required.");
        List<String> namedParameters = new ArrayList<>();
        parameterCount = JdbcParameterUtils.countParameterPlaceholders(sql, namedParameters);
        if (namedParameters.size() > 0) {
            if (parameterCount != namedParameters.size()) {
                throw new InvalidDataAccessApiUsageException(
                        "You can't use both named parameters and classic \"?\" placeholders: " + sql);
            }
            usingNamedParameters = true;
        }
        if (!usingNamedParameters) {
            Assert.notNull(itemPreparedStatementSetter,
                    "Using SQL statement with '?' placeholders requires an ItemPreparedStatementSetter");
        } else {
            Assert.notNull(itemSqlParameterSourceProvider,
                    "Using SQL statement with named parameters requires an ItemSqlParameterSourceProvider");
        }
    }

    @Override
    protected void writeToDataSource(Chunk<? extends T> items) throws Exception {
        if (usingNamedParameters && itemSqlParameterSourceProvider != null) {
            writeWithNamedParameters(items);
        } else if (!usingNamedParameters && itemPreparedStatementSetter != null) {
            writeWithPreparedStatement(items);
        } else {
            throw new IllegalArgumentException(
                    "Either ItemSqlParameterSourceProvider or ItemPreparedStatementSetter must be set based on SQL parameter type.");
        }
        // Validate updates if assertUpdates is enabled
        if (assertUpdates) {
            validateUpdates(items);
        }
    }

    private void writeWithNamedParameters(Chunk<? extends T> items) {
        SqlParameterSource[] batchArgs = new SqlParameterSource[items.size()];
        int i = 0;
        for (T item : items) {
            batchArgs[i++] = itemSqlParameterSourceProvider.createSqlParameterSource(item);
        }
        int[] updateCounts = namedParameterJdbcTemplate.batchUpdate(sql, batchArgs);
        for (int j = 0; j < updateCounts.length; j++) {
            if (updateCounts[j] == 0) {
                logger.warn("Item " + j + " did not update any rows.");
                throw new EmptyResultDataAccessException("Item " + j + " did not update any rows.", 1);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Successfully executed batch update with named parameters for " + items.size() + " items.");
        }
    }

    private void writeWithPreparedStatement(Chunk<? extends T> items) {
        int[] updateCounts = namedParameterJdbcTemplate.getJdbcOperations().execute(sql,
                new PreparedStatementCallback<int[]>() {
                    @Override
                    public int[] doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                        for (T item : items) {
                            itemPreparedStatementSetter.setValues(item, ps);
                            ps.addBatch();
                        }
                        return ps.executeBatch();
                    }
                });
        for (int i = 0; i < updateCounts.length; i++) {
            if (updateCounts[i] == 0) {
                logger.warn("Item " + i + " did not update any rows.");
                throw new EmptyResultDataAccessException("Item " + i + " did not update any rows.", 1);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Successfully executed batch update with prepared statement for " + items.size() + " items.");
        }
    }

    private void validateUpdates(Chunk<? extends T> items) {
        if (logger.isDebugEnabled()) {
            logger.debug("Validating updates for " + items.size() + " items.");
        }
    }

    @Override
    public void setAssertUpdates(boolean assertUpdates) {
        super.setAssertUpdates(assertUpdates);
        this.assertUpdates = assertUpdates;
    }
}