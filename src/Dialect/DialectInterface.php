<?php

namespace Graze\DataDb\Dialect;

use Graze\DataDb\TableNodeInterface;

interface DialectInterface
{
    /**
     * @param TableNodeInterface $old
     * @param TableNodeInterface $new
     *
     * @return array [sql, params]
     */
    public function getCreateTableLike(TableNodeInterface $old, TableNodeInterface $new);

    /**
     * @param TableNodeInterface $table
     * @param string             $column
     *
     * @return array [sql, params]
     */
    public function getDropColumn(TableNodeInterface $table, $column);

    /**
     * @param TableNodeInterface $table
     *
     * @return array [sql, params] where sql returns the table name
     */
    public function getDoesTableExist(TableNodeInterface $table);

    /**
     * Get delete table with join syntax where the table is a soft delete
     *
     * @param TableNodeInterface $source
     * @param TableNodeInterface $join
     * @param string             $on
     * @param string|null        $where
     *
     * @return array [sql, params]
     */
    public function getDeleteTableJoinSoftDelete(
        TableNodeInterface $source,
        TableNodeInterface $join,
        $on,
        $where = null
    );

    /**
     * Get delete table join
     *
     * @param TableNodeInterface $source
     * @param TableNodeInterface $join
     * @param string             $on
     * @param string|null        $where
     *
     * @return array [sql, params]
     */
    public function getDeleteTableJoin(
        TableNodeInterface $source,
        TableNodeInterface $join,
        $on,
        $where = null
    );

    /**
     * Delete entries from a table
     *
     * @param TableNodeInterface $table
     * @param string|null        $where
     *
     * @return array [sql, params]
     */
    public function getDeleteFromTableSoftDelete(TableNodeInterface $table, $where = null);

    /**
     * Delete entries from a table
     *
     * @param TableNodeInterface $table
     * @param string|null        $where
     *
     * @return array [sql, params]
     */
    public function getDeleteFromTable(TableNodeInterface $table, $where = null);

    /**
     * @param TableNodeInterface $from
     * @param TableNodeInterface $to
     *
     * @return array [sql, params]
     */
    public function getCopyTable(TableNodeInterface $from, TableNodeInterface $to);

    /**
     * @param TableNodeInterface $table
     * @param string[]           $columns
     * @param string[]           $primary
     * @param string[]           $index
     *
     * @return array [sql, params]
     */
    public function getCreateTable(TableNodeInterface $table, array $columns, array $primary, array $index);

    /**
     * @param array $column
     *
     * @return string
     */
    public function getColumnDefinition(array $column);

    /**
     * @param array $key
     *
     * @return string
     */
    public function getPrimaryKeyDefinition(array $key);

    /**
     * @param array $index
     *
     * @return string
     */
    public function getIndexDefinition(array $index);

    /**
     * @param TableNodeInterface $table
     *
     * @return array [sql, bind]
     */
    public function getDescribeTable(TableNodeInterface $table);

    /**
     * @param TableNodeInterface $table
     *
     * @return array [sql, bind]
     */
    public function getCreateSyntax(TableNodeInterface $table);

    /**
     * @param TableNodeInterface $table
     *
     * @return array [sql, bind]
     */
    public function getSelectSyntax(TableNodeInterface $table);

    /**
     * @param TableNodeInterface $table
     * @param array              $rows
     *
     * @return array [sql, bind]
     */
    public function getInsertSyntax(TableNodeInterface $table, array $rows);
}
