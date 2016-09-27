<?php

namespace Graze\DataDb\Dialect;

use Graze\DataDb\Formatter\SyntaxFormatter;
use Graze\DataDb\Formatter\SyntaxFormatterInterface;
use Graze\DataDb\TableNodeInterface;

class MysqlDialect extends AbstractDialect
{
    /**
     * @return string
     */
    public function getIdentifierQuote()
    {
        return '`';
    }

    /**
     * {@inheritdoc}
     */
    public function getCreateTableLike(TableNodeInterface $old, TableNodeInterface $new)
    {
        return [
            $this->format(
                'CREATE TABLE {new:schema|q}.{new:table|q} LIKE {old:schema|q}.{old:table|q}',
                ['old' => $old, 'new' => $new,]
            ),
            [],
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function getDeleteTableJoinSoftDelete(
        TableNodeInterface $source,
        TableNodeInterface $join,
        $on,
        $where = null
    ) {
        return [
            $this->format(
                'UPDATE {source:schema|q}.{source:table|q}
                JOIN {join:schema|q}.{join:table|q}
                ON {on}
                SET{softUpdated}
                    {source:softDeleted|q} = CURRENT_TIMESTAMP
                {where}',
                [
                    'source'      => $source,
                    'join'        => $join,
                    'on'          => $on,
                    'softUpdated' => ($source->getSoftUpdated() ?
                        $this->format(
                            ' {source:softUpdated|q} = CURRENT_TIMESTAMP,',
                            ['source' => $source]
                        ) : ''
                    ),
                    'where'       => ($where ? 'WHERE ' . $where : ''),
                ]
            ),
            [],
        ];
    }

    /**
     * @inheritdoc
     */
    public function getDeleteTableJoin(
        TableNodeInterface $source,
        TableNodeInterface $join,
        $on,
        $where = null
    ) {
        return [
            $this->format(
                'DELETE {source:schema|q}.{source:table|q}
                FROM {source:schema|q}.{source:table|q}
                JOIN {join:schema|q}.{join:table|q}
                ON {on}
                {where}',
                [
                    'source' => $source,
                    'join'   => $join,
                    'on'     => $on,
                    'where'  => ($where ? 'WHERE ' . $where : ''),
                ]
            ),
            [],
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function getDeleteFromTableSoftDelete(TableNodeInterface $table, $where = null)
    {
        return [
            $this->format(
                'UPDATE {table:schema|q}.{table:table|q}
                    SET{softUpdated}
                        {table:softDeleted|q} = CURRENT_TIMESTAMP
                    {where}',
                [
                    'table'       => $table,
                    'softUpdated' => ($table->getSoftUpdated() ?
                        $this->format(' {table:softUpdated|q} = CURRENT_TIMESTAMP,', ['table' => $table]) :
                        ''),
                    'where'       => ($where ? 'WHERE ' . $where : ''),
                ]
            ),
            [],
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function getCreateTable(TableNodeInterface $table, array $columns, array $primary, array $index)
    {
        $allColumns = array_merge($columns, $primary, $index);

        return [
            $this->format(
                "CREATE TABLE {table:schema|q}.{table:table|q} (\n  {allColumns}\n)",
                [
                    'table'      => $table,
                    'allColumns' => implode(",\n  ", $allColumns),
                ]
            ),
            [],
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function getColumnDefinition(array $column)
    {
        return $this->format(
            '{column|q} {type} {notnull}',
            array_merge($column, ['notnull' => $column['nullable'] ? 'NULL' : 'NOT NULL'])
        );
    }

    /**
     * {@inheritdoc}
     */
    public function getPrimaryKeyDefinition(array $key)
    {
        return $this->format('PRIMARY KEY ({column|q})', $key);
    }

    /**
     * {@inheritdoc}
     */
    public function getIndexDefinition(array $index)
    {
        return $this->format('KEY {column|q} ({column|q})', $index);
    }

    /**
     * {@inheritdoc}
     */
    public function getDescribeTable(TableNodeInterface $table)
    {
        return [$this->format('DESCRIBE {table:schema|q}.{table:table|q}', ['table' => $table]), []];
    }

    /**
     * {@inheritdoc}
     */
    public function getCreateSyntax(TableNodeInterface $table)
    {
        return [
            $this->format('SHOW CREATE TABLE {table:schema|q}.{table:table|q}', ['table' => $table]),
            [],
        ];
    }
}
