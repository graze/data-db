<?php

namespace Graze\DataDb;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataNode\NodeInterface;
use Traversable;

interface QueryNodeInterface extends NodeInterface
{
    #region Properties
    /**
     * @return AdapterInterface
     */
    public function getAdapter();

    /**
     * @param AdapterInterface $adapter
     *
     * @return static
     */
    public function setAdapter(AdapterInterface $adapter);

    /**
     * @return string
     */
    public function getSql();

    /**
     * @param string $sql
     *
     * @return static
     */
    public function setSql($sql);

    /**
     * @return array
     */
    public function getBind();

    /**
     * @param array $bind
     *
     * @return static
     */
    public function setBind(array $bind);

    /**
     * @return string[]
     */
    public function getColumns();

    /**
     * @param string[] $columns
     *
     * @return static
     */
    public function setColumns(array $columns);
    #endregion

    #region Query
    /**
     * @return mixed
     */
    public function query();

    /**
     * @return Traversable
     */
    public function fetch();

    /**
     * @return array
     */
    public function fetchAll();

    /**
     * @return array
     */
    public function fetchRow();

    /**
     * @return mixed
     */
    public function fetchOne();
    #endregion
}
