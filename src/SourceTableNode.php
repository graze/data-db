<?php

namespace Graze\DataDb;

class SourceTableNode extends TableNode implements SourceTableNodeInterface
{
    /** @var string */
    protected $where;

    /**
     * @param string $where
     *
     * @return static
     */
    public function setWhere($where)
    {
        $this->where = $where;
        return $this;
    }

    /**
     * @return string
     */
    public function getWhere()
    {
        return $this->where;
    }
}
