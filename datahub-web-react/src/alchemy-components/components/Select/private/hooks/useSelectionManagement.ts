import { isEqual } from 'lodash';
import { useCallback, useEffect, useState } from 'react';

interface UseSelectionManagementProps {
    initialValues: string[];
    values?: string[];
    onUpdate?: (values: string[]) => void;
    isMultiselect?: boolean;
    autocommit?: boolean;
}

interface Options {
    autocommit?: boolean;
}

interface UseSelectionManagementReturn {
    selectedValues: string[];
    stagedValues: string[];
    setSelectedValues: (values: string[]) => void;
    setStagedValues: (values: string[], options?: Options) => void;
    resetStagedValues: () => void;
    onValueChanged: (value: string) => void;
    clearSelection: (options?: Options) => void;
    commitSelection: () => void;
}

export const useSelectionManagement = ({
    initialValues,
    values,
    onUpdate,
    isMultiselect,
    autocommit,
}: UseSelectionManagementProps): UseSelectionManagementReturn => {
    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues || []);
    const [stagedValues, setInternalStagedValues] = useState<string[]>(initialValues || []);

    const updateSelectedValues = useCallback(
        (newValues: string[]) => {
            setSelectedValues(newValues);
            onUpdate?.(newValues);
        },
        [onUpdate],
    );

    const setStagedValues = useCallback(
        (newValues: string[], options?: Options) => {
            setInternalStagedValues(newValues);
            if (autocommit || options?.autocommit) {
                updateSelectedValues(newValues);
            }
        },
        [autocommit, updateSelectedValues],
    );

    // Sync both selected and staged when controlled values change
    useEffect(() => {
        if (values !== undefined && !isEqual(selectedValues, values)) {
            setSelectedValues(values);
            setInternalStagedValues(values);
        }
    }, [values]); // eslint-disable-line react-hooks/exhaustive-deps

    const onValueChanged = useCallback(
        (value: string) => {
            let newStagedValues: string[];

            if (stagedValues.includes(value)) {
                // Remove value
                newStagedValues = stagedValues.filter((v) => v !== value);
            } else if (!isMultiselect) {
                // Single select: replace
                newStagedValues = [value];
            } else {
                // Multi select: add
                newStagedValues = [...stagedValues, value];
            }

            setStagedValues(newStagedValues);
        },
        [stagedValues, isMultiselect, setStagedValues],
    );

    const clearSelection = useCallback(
        (options) => {
            setStagedValues([], options);
        },
        [setStagedValues],
    );

    const resetStagedValues = useCallback(() => {
        setInternalStagedValues(selectedValues);
    }, [selectedValues]);

    const commitSelection = useCallback(() => {
        // When autocommit is enabled, values are committed immediately via setStagedValues.
        // commitSelection is only needed when autocommit is disabled to apply staged changes.
        if (autocommit) {
            return;
        }
        updateSelectedValues(stagedValues);
    }, [autocommit, stagedValues, updateSelectedValues]);

    return {
        selectedValues,
        stagedValues,
        setSelectedValues,
        setStagedValues,
        resetStagedValues,
        onValueChanged,
        clearSelection,
        commitSelection,
    };
};
