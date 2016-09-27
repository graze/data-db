<?php

namespace Graze\DataDb;

interface SourceTableNodeInterface extends TableNodeInterface
{
    /**
     * @return string
     */
    public function getWhere();
}
